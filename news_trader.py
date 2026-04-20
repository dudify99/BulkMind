"""
NewsTrader — LLM-Powered News Trading Agent (Multi-Exchange)
Strategy: Monitor crypto news sources, analyze with Claude, trade high-impact
events on both Bulk and Hyperliquid simultaneously.
"""

import asyncio
import json
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple

import aiohttp

from ta import atr, compute_sl_tp, position_size
from db import (
    log_trade, close_trade, get_open_trades, get_agent_stats, log_issue,
    save_news_event, is_news_seen, mark_news_traded, get_conn, release_conn,
)
from reporter import Reporter
from agent_monitor import monitor
from config import (
    NEWS_EXCHANGES, NEWS_SYMBOLS, NEWS_POLL_INTERVAL_SEC,
    NEWS_MIN_IMPACT_SCORE, NEWS_ATR_MULT, NEWS_TP_RATIO,
    NEWS_MAX_POSITION_USD, NEWS_MAX_HOLD_MIN, NEWS_MAX_AGE_MIN,
    NEWS_PAPER_MODE, NEWS_LLM_MODEL, CRYPTOPANIC_API_KEY,
    ANTHROPIC_API_KEY, HL_SYMBOL_MAP,
    LUNARCRUSH_API_KEY, LUNARCRUSH_TOPICS, LUNARCRUSH_BASE_URL,
    SOCIALDATA_API_KEY, SOCIALDATA_QUERIES,
)

AGENT_NAME = "NewsTrader"

# ── RSS Feed sources ──────────────────────────────────────────
RSS_FEEDS = [
    ("coindesk",      "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("cointelegraph", "https://cointelegraph.com/rss"),
    ("theblock",      "https://www.theblock.co/rss.xml"),
]


class ExchangeVenue:
    """Lightweight wrapper pairing a name with its client + executor."""
    __slots__ = ("name", "client", "executor", "paper")

    def __init__(self, name: str, client, executor, paper: bool = True):
        self.name     = name
        self.client   = client
        self.executor = executor
        self.paper    = paper

    def resolve_symbol(self, symbol: str) -> str:
        """Map internal symbol (BTC-USD) to exchange-specific symbol."""
        if self.name == "hyperliquid":
            return HL_SYMBOL_MAP.get(symbol, symbol)
        return symbol


class NewsTrader:
    def __init__(self, venues: List[ExchangeVenue],
                 reporter: Reporter,
                 session: aiohttp.ClientSession):
        self.venues: List[ExchangeVenue] = venues
        self.reporter  = reporter
        self.session   = session
        self.open_trades: Dict[int, dict] = {}  # trade_id → trade info
        self._seen_ids: set = set()             # in-memory dedup cache

    # ── News Fetching ─────────────────────────────────────────

    async def _fetch_cryptopanic(self) -> List[dict]:
        """Fetch from CryptoPanic API. Free public endpoint works without an auth token."""
        params: dict = {"public": "true", "kind": "news", "filter": "hot"}
        if CRYPTOPANIC_API_KEY:
            params["auth_token"] = CRYPTOPANIC_API_KEY
        try:
            async with self.session.get(
                "https://cryptopanic.com/api/v1/posts/",
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                articles = []
                for item in data.get("results", []):
                    articles.append({
                        "id":           f"cp_{item.get('id', '')}",
                        "title":        item.get("title", ""),
                        "body":         item.get("title", ""),  # free tier has no body
                        "source":       "cryptopanic",
                        "url":          item.get("url", ""),
                        "published_at": item.get("published_at", ""),
                    })
                return articles
        except Exception as e:
            print(f"  [NewsTrader] CryptoPanic fetch error: {e}")
            return []

    async def _fetch_coingecko(self) -> List[dict]:
        """Fetch from CoinGecko /news endpoint (free, no key required)."""
        try:
            async with self.session.get(
                "https://api.coingecko.com/api/v3/news",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                articles = []
                for item in data.get("data", []):
                    articles.append({
                        "id":           f"cg_{item.get('id', '')}",
                        "title":        item.get("title", ""),
                        "body":         item.get("description", ""),
                        "source":       "coingecko",
                        "url":          item.get("url", ""),
                        "published_at": item.get("updated_at", ""),
                    })
                return articles
        except Exception as e:
            print(f"  [NewsTrader] CoinGecko fetch error: {e}")
            return []

    async def _fetch_rss(self, name: str, url: str) -> List[dict]:
        """Fetch and parse an RSS feed using stdlib xml.etree.ElementTree."""
        try:
            async with self.session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "BulkMind/1.0 NewsTrader"},
            ) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
                root = ET.fromstring(text)
                articles = []
                for item in root.iter("item"):
                    title = (item.findtext("title") or "").strip()
                    link  = (item.findtext("link")  or "").strip()
                    desc  = (item.findtext("description") or "").strip()
                    pub   = (item.findtext("pubDate") or "").strip()
                    guid  = (item.findtext("guid") or link).strip()
                    if not title:
                        continue
                    article_id = f"{name}_{hashlib.md5(guid.encode()).hexdigest()[:12]}"
                    articles.append({
                        "id":           article_id,
                        "title":        title,
                        "body":         desc[:500] if desc else title,
                        "source":       name,
                        "url":          link,
                        "published_at": pub,
                    })
                return articles
        except Exception as e:
            print(f"  [NewsTrader] RSS {name} fetch error: {e}")
            return []

    # ── LunarCrush — Free CT Social Posts + News ────────────────

    async def _fetch_lunarcrush_posts(self) -> List[dict]:
        """Fetch social posts (CT tweets) for tracked topics via LunarCrush v4 public API."""
        if not LUNARCRUSH_API_KEY:
            return []

        headers = {
            "Authorization": f"Bearer {LUNARCRUSH_API_KEY}",
            "User-Agent":    "BulkMind/1.0 NewsTrader",
        }
        all_posts: List[dict] = []

        for topic in LUNARCRUSH_TOPICS:
            try:
                async with self.session.get(
                    f"{LUNARCRUSH_BASE_URL}/public/topic/{topic}/posts/v1",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 429:
                        print("  [NewsTrader] LunarCrush rate limited")
                        return all_posts
                    if resp.status != 200:
                        continue
                    data = await resp.json()

                    for post in data.get("data", []):
                        post_id   = str(post.get("id", ""))
                        text      = post.get("post_title", "") or post.get("title", "")
                        author    = post.get("creator_display_name", "") or post.get("creator_name", "")
                        post_url  = post.get("post_link", "") or post.get("url", "")
                        created   = post.get("post_created", "") or post.get("time", "")
                        interactions = post.get("interactions_total", 0) or 0

                        if not text:
                            continue
                        # Skip low-engagement posts
                        if interactions < 5:
                            continue

                        all_posts.append({
                            "id":           f"lc_{topic}_{post_id}",
                            "title":        text[:280],
                            "body":         text,
                            "source":       "lunarcrush",
                            "url":          post_url,
                            "published_at": str(created),
                            "author":       f"@{author}" if author else "",
                            "engagement":   interactions,
                        })
            except Exception as e:
                print(f"  [NewsTrader] LunarCrush posts/{topic} error: {e}")

        return all_posts

    async def _fetch_lunarcrush_news(self) -> List[dict]:
        """Fetch aggregated crypto news for topics via LunarCrush v4 public API."""
        if not LUNARCRUSH_API_KEY:
            return []

        headers = {
            "Authorization": f"Bearer {LUNARCRUSH_API_KEY}",
            "User-Agent":    "BulkMind/1.0 NewsTrader",
        }
        all_news: List[dict] = []

        for topic in LUNARCRUSH_TOPICS:
            try:
                async with self.session.get(
                    f"{LUNARCRUSH_BASE_URL}/public/topic/{topic}/news/v1",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 429:
                        print("  [NewsTrader] LunarCrush rate limited")
                        return all_news
                    if resp.status != 200:
                        continue
                    data = await resp.json()

                    for item in data.get("data", []):
                        news_id = str(item.get("id", ""))
                        title   = item.get("post_title", "") or item.get("title", "")
                        body    = item.get("description", "") or title
                        url     = item.get("post_link", "") or item.get("url", "")
                        created = item.get("post_created", "") or item.get("time", "")

                        if not title:
                            continue

                        all_news.append({
                            "id":           f"lcn_{topic}_{news_id}",
                            "title":        title[:280],
                            "body":         body[:500],
                            "source":       "lunarcrush_news",
                            "url":          url,
                            "published_at": str(created),
                            "author":       "",
                        })
            except Exception as e:
                print(f"  [NewsTrader] LunarCrush news/{topic} error: {e}")

        return all_news

    # ── SocialData.tools — Full Tweet Search (optional) ───────

    async def _fetch_socialdata(self) -> List[dict]:
        """Fetch tweets via SocialData.tools API (pay-as-you-go, optional)."""
        if not SOCIALDATA_API_KEY:
            return []

        headers = {
            "Authorization": f"Bearer {SOCIALDATA_API_KEY}",
            "Accept":        "application/json",
        }
        all_tweets: List[dict] = []

        for query in SOCIALDATA_QUERIES:
            try:
                async with self.session.get(
                    "https://api.socialdata.tools/twitter/search",
                    headers=headers,
                    params={"query": query},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 429:
                        print("  [NewsTrader] SocialData rate limited")
                        return all_tweets
                    if resp.status != 200:
                        continue
                    data = await resp.json()

                    for tweet in data.get("tweets", []):
                        tweet_id = str(tweet.get("id_str", "") or tweet.get("id", ""))
                        text     = tweet.get("full_text", "") or tweet.get("text", "")
                        user     = tweet.get("user", {})
                        username = user.get("screen_name", "")
                        metrics  = tweet.get("public_metrics", {})
                        likes    = (metrics.get("like_count", 0)
                                    or tweet.get("favorite_count", 0))
                        rts      = (metrics.get("retweet_count", 0)
                                    or tweet.get("retweet_count", 0))

                        if not text or (likes + rts < 5):
                            continue

                        all_tweets.append({
                            "id":           f"sd_{tweet_id}",
                            "title":        text[:280],
                            "body":         text,
                            "source":       "socialdata",
                            "url":          f"https://x.com/{username}/status/{tweet_id}",
                            "published_at": tweet.get("created_at", ""),
                            "author":       f"@{username}" if username else "",
                            "engagement":   likes + rts,
                        })
            except Exception as e:
                print(f"  [NewsTrader] SocialData search error: {e}")

        return all_tweets

    async def fetch_news(self) -> List[dict]:
        """Aggregate + deduplicate news from all sources (LunarCrush, SocialData, CryptoPanic, CoinGecko, RSS)."""
        tasks = [
            self._fetch_lunarcrush_posts(),
            self._fetch_lunarcrush_news(),
            self._fetch_socialdata(),
            self._fetch_cryptopanic(),
            self._fetch_coingecko(),
            *[self._fetch_rss(name, url) for name, url in RSS_FEEDS],
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_articles: List[dict] = []
        for r in results:
            if isinstance(r, list):
                all_articles.extend(r)

        fresh = []
        for article in all_articles:
            if article["id"] in self._seen_ids:
                continue
            if is_news_seen(article["source"], article["id"]):
                self._seen_ids.add(article["id"])
                continue
            fresh.append(article)

        return fresh

    # ── LLM Analysis ─────────────────────────────────────────

    async def analyze_news(self, article: dict) -> Optional[dict]:
        """
        Call Claude to classify sentiment, impact score, and affected symbols.
        Returns analysis dict or None if neutral/low-impact/below threshold.
        """
        if not ANTHROPIC_API_KEY:
            return None

        title  = article.get("title", "")
        body   = article.get("body",  "")[:500]
        source = article.get("source", "unknown")
        pub    = article.get("published_at", "")
        author = article.get("author", "")

        source_line = f"Source: {source}"
        if author:
            source_line += f" ({author})"

        prompt = (
            "You are a crypto news trading analyst. Analyze this article and determine "
            "its trading impact.\n\n"
            f"Headline: {title}\n"
            f"Summary: {body}\n"
            f"{source_line}\n"
            f"Published: {pub}\n\n"
            "Tradeable symbols: BTC-USD, ETH-USD, SOL-USD\n\n"
            'Respond in EXACTLY this JSON format (no markdown, no extra text):\n'
            '{"sentiment": "BUY" or "SELL" or "NEUTRAL", "impact": 1-10, '
            '"symbols": ["SYM-USD"], "reasoning": "one sentence"}\n\n'
            "Scoring guide:\n"
            "- impact 1-3: routine news (minor partnership, roadmap update)\n"
            "- impact 4-6: notable but uncertain direction\n"
            "- impact 7-8: significant market-moving event (major hack, major institutional buy, "
            "major regulatory action)\n"
            "- impact 9-10: black swan (exchange collapse, blanket ban, critical exploit)\n\n"
            "Rules:\n"
            "- Only list symbols you are confident are affected\n"
            "- BUY = bullish catalyst, SELL = bearish catalyst, NEUTRAL = unclear direction\n"
            "- If no clear crypto impact, use NEUTRAL with impact <= 3"
        )

        try:
            async with self.session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "Content-Type":      "application/json",
                    "x-api-key":         ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                },
                json={
                    "model":    NEWS_LLM_MODEL,
                    "max_tokens": 256,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    err = await resp.text()
                    print(f"  [NewsTrader] Claude API error {resp.status}: {err[:120]}")
                    return None
                data = await resp.json()
                raw_text = data["content"][0]["text"].strip()
                analysis = json.loads(raw_text)
        except json.JSONDecodeError as e:
            print(f"  [NewsTrader] Claude JSON parse error: {e}")
            return None
        except Exception as e:
            print(f"  [NewsTrader] Claude API call failed: {e}")
            return None

        # Apply threshold filters
        if analysis.get("impact", 0) < NEWS_MIN_IMPACT_SCORE:
            return None
        if analysis.get("sentiment", "NEUTRAL") == "NEUTRAL":
            return None
        tradeable = [s for s in analysis.get("symbols", []) if s in NEWS_SYMBOLS]
        if not tradeable:
            return None

        analysis["symbols"] = tradeable
        analysis["article"] = article
        return analysis

    # ── Signal Generation (per exchange) ──────────────────────

    async def get_signal(self, symbol: str, analysis: dict,
                         venue: ExchangeVenue) -> Optional[dict]:
        """
        Build a trade signal for a specific exchange venue.
        Uses that venue's candles + ticker for accurate pricing.
        """
        direction = "BUY" if analysis["sentiment"] == "BUY" else "SELL"
        ex_symbol = venue.resolve_symbol(symbol)

        raw = await venue.client.get_candles(ex_symbol, interval="15m", limit=20)
        if len(raw) < 5:
            return None

        atr_vals = atr(raw, period=14)
        if not atr_vals:
            return None
        current_atr = atr_vals[-1]

        ticker = await venue.client.get_ticker(ex_symbol)
        if not ticker:
            return None
        entry = float(
            ticker.get("lastPrice") or
            ticker.get("last_price") or
            ticker.get("price", 0)
        )
        if not entry:
            return None

        levels = compute_sl_tp(
            direction, entry, current_atr, NEWS_ATR_MULT, NEWS_TP_RATIO
        )
        size = position_size(NEWS_MAX_POSITION_USD, entry, levels["sl"])

        return {
            "symbol":    symbol,         # internal symbol (BTC-USD)
            "ex_symbol": ex_symbol,      # exchange-specific symbol
            "exchange":  venue.name,
            "direction": direction,
            "entry":     entry,
            "sl":        levels["sl"],
            "tp":        levels["tp"],
            "size":      size,
            "atr":       round(current_atr, 6),
            "rr_ratio":  levels["rr_ratio"],
            "impact":    analysis["impact"],
            "reasoning": analysis["reasoning"],
            "headline":  analysis["article"]["title"],
            "source":    analysis["article"]["source"],
            "timestamp": datetime.utcnow().isoformat(),
        }

    # ── Trade Execution (per exchange) ────────────────────────

    async def execute_signal(self, signal: dict, venue: ExchangeVenue,
                             news_event_id: int) -> Optional[int]:
        symbol    = signal["symbol"]
        ex_symbol = signal["ex_symbol"]
        side      = signal["direction"]
        agent_tag = f"{AGENT_NAME}:{venue.name}"

        # Max one position per symbol per exchange
        for t in get_open_trades(agent_tag):
            if t["symbol"] == symbol:
                print(f"  [{venue.name}/{symbol}] Already have open position, skipping")
                return None

        result = await venue.executor.place_bracket(
            symbol      = ex_symbol,
            side        = side,
            entry_price = signal["entry"],
            size        = signal["size"],
            sl_price    = signal["sl"],
            tp_price    = signal["tp"],
        )

        if not result:
            log_issue("HIGH", "AGENT_ERROR",
                      f"NewsTrader/{venue.name} failed to place order on {symbol}",
                      json.dumps(signal))
            return None

        trade_id = log_trade(
            agent       = agent_tag,
            symbol      = symbol,
            side        = side,
            entry_price = signal["entry"],
            size        = signal["size"],
            sl          = signal["sl"],
            tp          = signal["tp"],
            signal_data = signal,
            paper       = venue.paper,
            order_id    = result.get("order_id", ""),
        )

        mark_news_traded(news_event_id, trade_id)

        self.open_trades[trade_id] = {
            "symbol":   symbol,
            "exchange": venue.name,
            "side":     side,
            "entry":    signal["entry"],
            "sl":       signal["sl"],
            "tp":       signal["tp"],
            "size":     signal["size"],
            "entry_ts": datetime.utcnow(),
        }

        await self.reporter.send(
            f"📰 *NewsTrader — New Trade*\n"
            f"Exchange: `{venue.name}`\n"
            f"Symbol: `{symbol}`\n"
            f"Side: `{side}`\n"
            f"Entry: `{signal['entry']}`\n"
            f"SL: `{signal['sl']}`\n"
            f"TP: `{signal['tp']}`\n"
            f"Impact: `{signal['impact']}/10`\n"
            f"Source: `{signal['source']}`\n"
            f"Headline: _{signal['headline'][:80]}_\n"
            f"Reason: _{signal['reasoning']}_\n"
            f"Paper: `{venue.paper}`"
        )
        return trade_id

    # ── Trade Management ──────────────────────────────────────

    def _venue_by_name(self, name: str) -> Optional[ExchangeVenue]:
        for v in self.venues:
            if v.name == name:
                return v
        return None

    async def manage_open_trades(self):
        """Close positions on SL/TP hit or when max hold time is exceeded."""
        if not self.open_trades:
            return

        now = datetime.utcnow()
        for trade_id, trade in list(self.open_trades.items()):
            venue = self._venue_by_name(trade["exchange"])
            if not venue:
                continue

            ex_symbol = venue.resolve_symbol(trade["symbol"])
            ticker = await venue.client.get_ticker(ex_symbol)
            if not ticker:
                continue

            price = float(
                ticker.get("lastPrice") or
                ticker.get("last_price") or
                ticker.get("price", 0)
            )
            if not price:
                continue

            side   = trade["side"]
            status = None

            if side == "BUY":
                if price <= trade["sl"]:
                    status = "LOSS"
                elif price >= trade["tp"]:
                    status = "WIN"
            else:
                if price >= trade["sl"]:
                    status = "LOSS"
                elif price <= trade["tp"]:
                    status = "WIN"

            # Time-based force-close after NEWS_MAX_HOLD_MIN
            if not status:
                elapsed_min = (now - trade["entry_ts"]).total_seconds() / 60
                if elapsed_min >= NEWS_MAX_HOLD_MIN:
                    status = "WIN" if (
                        (side == "BUY"  and price > trade["entry"]) or
                        (side == "SELL" and price < trade["entry"])
                    ) else "LOSS"
                    print(
                        f"  [{trade['exchange']}/{trade['symbol']}] "
                        f"NewsTrader time-exit after {elapsed_min:.0f} min → {status}"
                    )

            if status:
                pnl = close_trade(trade_id, price, status)
                del self.open_trades[trade_id]

                emoji = "✅" if status == "WIN" else "❌"
                await self.reporter.send(
                    f"{emoji} *NewsTrader — Trade Closed*\n"
                    f"Exchange: `{trade['exchange']}`\n"
                    f"Symbol: `{trade['symbol']}`\n"
                    f"Status: `{status}`\n"
                    f"Exit: `{price}`\n"
                    f"PnL: `${pnl:.2f}`\n"
                    f"Trade ID: `{trade_id}`"
                )

    # ── Performance Report ────────────────────────────────────

    async def report_performance(self):
        """Report performance across all venues."""
        for venue in self.venues:
            agent_tag = f"{AGENT_NAME}:{venue.name}"
            stats = get_agent_stats(agent_tag)
            if not stats or not stats.get("total"):
                continue

            total  = stats["total"] or 0
            wins   = stats["wins"] or 0
            losses = stats["losses"] or 0
            pnl    = stats["total_pnl"] or 0
            wr     = (wins / total * 100) if total > 0 else 0

            await self.reporter.send(
                f"📰 *NewsTrader Performance ({venue.name})*\n"
                f"Total Trades: `{total}`\n"
                f"Wins: `{wins}` | Losses: `{losses}`\n"
                f"Win Rate: `{wr:.1f}%`\n"
                f"Total PnL: `${pnl:.2f}`\n"
                f"Avg PnL%: `{stats.get('avg_pnl_pct', 0):.2f}%`"
            )

    # ── EvoSkill: Export failure trajectories ─────────────────

    def export_failure_trajectories(self,
                                    output_path: str = "data/news_failures.json"):
        """Export losing trades from all venues as EvoSkill failure trajectories."""
        conn_db = get_conn()
        rows = conn_db.execute(
            "SELECT * FROM trades WHERE agent LIKE 'NewsTrader%' AND status='LOSS' "
            "ORDER BY ts DESC LIMIT 100",
        ).fetchall()
        conn_db.close()

        trajectories = []
        for row in rows:
            d = dict(row)
            signal = json.loads(d.get("signal_data") or "{}")
            trajectories.append({
                "question":     f"Should I have traded this {d['side']} news signal on {d['symbol']}?",
                "ground_truth": "NO",
                "agent_answer": "YES",
                "context": {
                    "entry":    d["entry_price"],
                    "sl":       d["sl_price"],
                    "tp":       d["tp_price"],
                    "exit":     d["exit_price"],
                    "pnl_pct":  d["pnl_pct"],
                    "exchange": signal.get("exchange", "unknown"),
                    "signal":   signal,
                },
            })

        from pathlib import Path
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(trajectories, f, indent=2)

        print(f"📦 NewsTrader: exported {len(trajectories)} failure trajectories → {output_path}")
        return output_path

    # ── Main Loop ─────────────────────────────────────────────

    async def run(self):
        venue_names = [v.name for v in self.venues]
        print(f"📰 {AGENT_NAME} started — Exchanges: {venue_names}")
        scan_count = 0

        while True:
            try:
                monitor.heartbeat(AGENT_NAME)
                print(f"\n🔍 [{AGENT_NAME}] Fetching news...")

                articles = await self.fetch_news()
                print(f"  [{AGENT_NAME}] {len(articles)} new article(s) to analyze")

                for article in articles:
                    # Mark seen immediately to prevent double-processing
                    self._seen_ids.add(article["id"])
                    save_news_event(
                        source     = article["source"],
                        article_id = article["id"],
                        title      = article["title"],
                    )

                    analysis = await self.analyze_news(article)
                    if not analysis:
                        continue

                    print(
                        f"  📡 HIGH IMPACT [{analysis['impact']}/10] "
                        f"{analysis['sentiment']} — {article['title'][:60]}"
                    )

                    # Persist enriched record
                    save_news_event(
                        source     = article["source"],
                        article_id = article["id"],
                        title      = article["title"],
                        sentiment  = analysis["sentiment"],
                        impact     = analysis["impact"],
                        symbols    = analysis["symbols"],
                    )

                    # Look up the saved event id
                    conn = get_conn()
                    row = conn.execute(
                        "SELECT id FROM news_events WHERE source=? AND article_id=?",
                        (article["source"], article["id"])
                    ).fetchone()
                    release_conn(conn)
                    event_id = row["id"] if row else 0

                    # Trade on all venues in parallel
                    for symbol in analysis["symbols"]:
                        tasks = []
                        for venue in self.venues:
                            tasks.append(
                                self._signal_and_execute(
                                    symbol, analysis, venue, event_id
                                )
                            )
                        await asyncio.gather(*tasks, return_exceptions=True)

                # Check open trades every poll cycle
                await self.manage_open_trades()

                scan_count += 1
                if scan_count % 20 == 0:
                    await self.report_performance()

                if scan_count % 100 == 0:
                    self.export_failure_trajectories()

            except Exception as e:
                print(f"NewsTrader error: {e}")
                log_issue("HIGH", "AGENT_ERROR",
                          "NewsTrader runtime error", str(e))

            await asyncio.sleep(NEWS_POLL_INTERVAL_SEC)

    async def _signal_and_execute(self, symbol: str, analysis: dict,
                                  venue: ExchangeVenue, event_id: int):
        """Helper: generate signal and execute for one venue."""
        signal = await self.get_signal(symbol, analysis, venue)
        if signal:
            await self.execute_signal(signal, venue, event_id)
