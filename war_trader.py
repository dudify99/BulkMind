"""
WarTrader — Geopolitical Event Trading Agent (Multi-Exchange)
Strategy: Monitor geopolitical news (wars, sanctions, tariffs, coups),
classify severity with Claude, trade crypto on risk-on/risk-off dynamics.
RISK_OFF → SELL, RISK_ON → BUY. Wider SL (2x ATR), 3:1 R:R.
"""

import asyncio, json, time, re, hashlib, aiohttp
from typing import Optional, List, Dict
from datetime import datetime, timedelta
from news_trader import ExchangeVenue
from reporter import Reporter
from ta import atr, compute_sl_tp, position_size
from db import (
    log_trade, close_trade, get_open_trades, get_agent_stats, log_issue,
    save_macro_event, mark_macro_traded, is_news_seen, save_news_event,
)
from config import (
    WAR_SYMBOLS, WAR_CHECK_SEC, WAR_POSITION_USD,
    WAR_MIN_SEVERITY, WAR_MAX_HOLD_MIN, WAR_PAPER_MODE,
    WAR_LLM_MODEL, WAR_KEYWORDS, ANTHROPIC_API_KEY,
    CRYPTOPANIC_API_KEY, HL_SYMBOL_MAP,
)

AGENT_NAME = "WarTrader"
GEO_RSS_FEEDS = [("reuters_world", "https://feeds.reuters.com/Reuters/worldNews"),
                  ("bbc_news",      "http://feeds.bbci.co.uk/news/rss.xml")]

class WarTrader:
    """Trades crypto based on geopolitical event classification."""

    def __init__(self, venues: List[ExchangeVenue],
                 reporter: Reporter, session: aiohttp.ClientSession):
        self.venues: List[ExchangeVenue] = venues
        self.reporter = reporter
        self.session  = session
        self._seen_ids: set = set()
        self.open_trades: Dict[int, dict] = {}

    # ── News Fetching ─────────────────────────────────────────
    async def _fetch_cryptopanic(self) -> List[dict]:
        """Fetch hot news from CryptoPanic API."""
        params: dict = {"filter": "hot", "kind": "news"}
        if CRYPTOPANIC_API_KEY:
            params["auth_token"] = CRYPTOPANIC_API_KEY
        else:
            params["public"] = "true"
        try:
            async with self.session.get(
                "https://cryptopanic.com/api/v1/posts/", params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                return [{"id": f"cp_war_{it.get('id','')}", "title": it.get("title",""),
                         "body": it.get("title",""), "source": "cryptopanic",
                         "url": it.get("url",""), "published_at": it.get("published_at","")}
                        for it in data.get("results", [])]
        except Exception as e:
            print(f"  [WarTrader] CryptoPanic fetch error: {e}")
            return []
    async def _fetch_rss(self, name: str, url: str) -> List[dict]:
        """Fetch and parse RSS feed with simple regex (no feedparser)."""
        try:
            async with self.session.get(
                url, timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "BulkMind/1.0 WarTrader"},
            ) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
                articles = []
                for item_xml in re.findall(r"<item[^>]*>(.*?)</item>", text, re.DOTALL):
                    title_m = re.search(r"<title[^>]*>(.*?)</title>", item_xml, re.DOTALL)
                    link_m  = re.search(r"<link[^>]*>(.*?)</link>", item_xml, re.DOTALL)
                    title = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1",
                                   title_m.group(1).strip() if title_m else "")
                    link  = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1",
                                   link_m.group(1).strip() if link_m else "")
                    if not title:
                        continue
                    aid = f"{name}_{hashlib.md5((link or title).encode()).hexdigest()[:12]}"
                    articles.append({"id": aid, "title": title, "body": title,
                                     "source": name, "url": link, "published_at": ""})
                return articles
        except Exception as e:
            print(f"  [WarTrader] RSS {name} fetch error: {e}")
            return []
    def _matches_keywords(self, text: str) -> bool:
        lower = text.lower()
        return any(kw in lower for kw in WAR_KEYWORDS)
    async def fetch_geopolitical_news(self) -> List[dict]:
        """Aggregate geopolitical news from all sources, filter by keywords, dedup."""
        tasks = [self._fetch_cryptopanic(),
                 *[self._fetch_rss(n, u) for n, u in GEO_RSS_FEEDS]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_articles: List[dict] = []
        for r in results:
            if isinstance(r, list):
                all_articles.extend(r)
        geo = [a for a in all_articles
               if self._matches_keywords(a.get("title","") + " " + a.get("body",""))]
        fresh = []
        for article in geo:
            if article["id"] in self._seen_ids:
                continue
            if is_news_seen(article["source"], article["id"]):
                self._seen_ids.add(article["id"])
                continue
            fresh.append(article)
        return fresh

    # ── LLM Classification ────────────────────────────────────
    async def classify_event(self, article: dict) -> Optional[dict]:
        """Use Claude to classify geopolitical event severity and direction."""
        if not ANTHROPIC_API_KEY:
            return None
        title, body = article.get("title",""), article.get("body","")[:500]
        source = article.get("source", "unknown")
        prompt = (
            "You are a geopolitical risk analyst for crypto markets. "
            "Classify this event:\n\n"
            f"Headline: {title}\nSummary: {body}\nSource: {source}\n\n"
            "Respond in EXACTLY this JSON:\n"
            '{"severity": 1-10, "risk_type": "RISK_OFF"|"RISK_ON"|"NEUTRAL", '
            '"assets": ["BTC-USD"], "reasoning": "one sentence"}\n\n'
            "Rules:\n"
            "- RISK_OFF (sell BTC): active conflict escalation, new sanctions, nuclear threats\n"
            "- RISK_ON (buy BTC): ceasefire, de-escalation, sanctions relief, safe haven demand\n"
            "- severity 1-3: minor diplomatic tension\n"
            "- severity 7-8: major geopolitical shift\n"
            "- severity 9-10: black swan (war declaration, nuclear event)")
        try:
            async with self.session.post(
                "https://api.anthropic.com/v1/messages",
                headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_API_KEY,
                         "anthropic-version": "2023-06-01"},
                json={"model": WAR_LLM_MODEL, "max_tokens": 256,
                      "messages": [{"role": "user", "content": prompt}]},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    print(f"  [WarTrader] Claude API error {resp.status}: {(await resp.text())[:120]}")
                    return None
                data = await resp.json()
                classification = json.loads(data["content"][0]["text"].strip())
        except json.JSONDecodeError as e:
            print(f"  [WarTrader] Claude JSON parse error: {e}"); return None
        except Exception as e:
            print(f"  [WarTrader] Claude API call failed: {e}"); return None
        if classification.get("severity", 0) < WAR_MIN_SEVERITY:
            return None
        if classification.get("risk_type", "NEUTRAL") == "NEUTRAL":
            return None
        tradeable = [a for a in classification.get("assets", []) if a in WAR_SYMBOLS]
        classification["assets"]  = tradeable or ["BTC-USD"]
        classification["article"] = article
        return classification

    # ── Signal Generation ─────────────────────────────────────
    async def get_signal(self, classification: dict,
                         venue: ExchangeVenue) -> Optional[dict]:
        """Build trade signal: RISK_OFF->SELL, RISK_ON->BUY. 2x ATR SL, 3:1 R:R."""
        risk_type = classification["risk_type"]
        direction = "SELL" if risk_type == "RISK_OFF" else "BUY"
        symbol, ex_symbol = classification["assets"][0], venue.resolve_symbol(classification["assets"][0])
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
        entry = float(ticker.get("lastPrice") or ticker.get("last_price") or ticker.get("price", 0))
        if not entry:
            return None
        levels = compute_sl_tp(direction, entry, current_atr, 2.0, 3.0)
        size = position_size(WAR_POSITION_USD, entry, levels["sl"])
        return {
            "symbol": symbol, "ex_symbol": ex_symbol, "exchange": venue.name,
            "direction": direction, "entry": entry, "sl": levels["sl"], "tp": levels["tp"],
            "size": size, "atr": round(current_atr, 6), "rr_ratio": levels["rr_ratio"],
            "risk_type": risk_type, "severity": classification.get("severity", 0),
            "reasoning": classification.get("reasoning", ""),
            "headline": classification["article"]["title"],
            "source": classification["article"]["source"],
            "timestamp": datetime.utcnow().isoformat(),
        }

    # ── Trade Execution ───────────────────────────────────────
    async def execute_signal(self, signal: dict, venue: ExchangeVenue,
                             event_id: int) -> Optional[int]:
        """Place bracket order and record the trade."""
        symbol, side = signal["symbol"], signal["direction"]
        agent_tag = f"{AGENT_NAME}:{venue.name}"
        for t in get_open_trades(agent_tag):
            if t["symbol"] == symbol:
                print(f"  [{venue.name}/{symbol}] Already have open position, skipping")
                return None
        result = await venue.executor.place_bracket(
            symbol=signal["ex_symbol"], side=side, entry_price=signal["entry"],
            size=signal["size"], sl_price=signal["sl"], tp_price=signal["tp"])
        if not result:
            log_issue("HIGH", "AGENT_ERROR",
                      f"WarTrader/{venue.name} failed to place order on {symbol}",
                      json.dumps(signal))
            return None
        trade_id = log_trade(
            agent=agent_tag, symbol=symbol, side=side, entry_price=signal["entry"],
            size=signal["size"], sl=signal["sl"], tp=signal["tp"],
            signal_data=signal, paper=venue.paper, order_id=result.get("order_id", ""))
        mark_macro_traded(event_id, trade_id)
        self.open_trades[trade_id] = {
            "symbol": symbol, "exchange": venue.name, "side": side,
            "entry": signal["entry"], "sl": signal["sl"], "tp": signal["tp"],
            "size": signal["size"], "entry_ts": datetime.utcnow()}
        await self.reporter.send(
            f"⚔️ *WarTrader — New Trade*\n"
            f"Exchange: `{venue.name}`  Symbol: `{symbol}`\n"
            f"Side: `{side}` ({signal['risk_type']})  Severity: `{signal['severity']}/10`\n"
            f"Entry: `{signal['entry']}`  SL: `{signal['sl']}`  TP: `{signal['tp']}`\n"
            f"Source: `{signal['source']}`\n"
            f"Headline: _{signal['headline'][:80]}_\n"
            f"Reason: _{signal['reasoning']}_  Paper: `{venue.paper}`")
        return trade_id

    # ── Trade Management ──────────────────────────────────────
    def _venue_by_name(self, name: str) -> Optional[ExchangeVenue]:
        for v in self.venues:
            if v.name == name:
                return v
        return None

    async def manage_open_trades(self):
        """Close positions on SL/TP hit or time-based exit after WAR_MAX_HOLD_MIN."""
        if not self.open_trades:
            return
        now = datetime.utcnow()
        for trade_id, trade in list(self.open_trades.items()):
            venue = self._venue_by_name(trade["exchange"])
            if not venue:
                continue
            ex_sym = venue.resolve_symbol(trade["symbol"])
            ticker = await venue.client.get_ticker(ex_sym)
            if not ticker:
                continue
            price = float(ticker.get("lastPrice") or ticker.get("last_price") or ticker.get("price", 0))
            if not price:
                continue
            side, status = trade["side"], None
            if side == "BUY":
                if price <= trade["sl"]:    status = "LOSS"
                elif price >= trade["tp"]:  status = "WIN"
            else:
                if price >= trade["sl"]:    status = "LOSS"
                elif price <= trade["tp"]:  status = "WIN"
            if not status:
                elapsed = (now - trade["entry_ts"]).total_seconds() / 60
                if elapsed >= WAR_MAX_HOLD_MIN:
                    status = "WIN" if ((side == "BUY" and price > trade["entry"]) or
                                       (side == "SELL" and price < trade["entry"])) else "LOSS"
                    print(f"  [{trade['exchange']}/{trade['symbol']}] "
                          f"WarTrader time-exit after {elapsed:.0f} min -> {status}")
            if status:
                pnl = close_trade(trade_id, price, status)
                del self.open_trades[trade_id]
                emoji = "✅" if status == "WIN" else "❌"
                await self.reporter.send(
                    f"{emoji} *WarTrader — Trade Closed*\n"
                    f"Exchange: `{trade['exchange']}`  Symbol: `{trade['symbol']}`\n"
                    f"Status: `{status}`  Exit: `{price}`  PnL: `${pnl:.2f}`\n"
                    f"Trade ID: `{trade_id}`")

    # ── Performance Report ────────────────────────────────────

    async def report_performance(self):
        for venue in self.venues:
            agent_tag = f"{AGENT_NAME}:{venue.name}"
            stats = get_agent_stats(agent_tag)
            if not stats or not stats.get("total"):
                continue
            total, wins = stats["total"] or 0, stats["wins"] or 0
            losses, pnl = stats["losses"] or 0, stats["total_pnl"] or 0
            wr = (wins / total * 100) if total > 0 else 0
            await self.reporter.send(
                f"⚔️ *WarTrader Performance ({venue.name})*\n"
                f"Trades: `{total}` | W: `{wins}` L: `{losses}` | WR: `{wr:.1f}%`\n"
                f"PnL: `${pnl:.2f}` | Avg: `{stats.get('avg_pnl_pct', 0):.2f}%`")

    # ── EvoSkill: Export failure trajectories ─────────────────

    def export_failure_trajectories(self, output_path: str = "data/war_failures.json"):
        """Export losing trades as EvoSkill failure trajectories."""
        from db import get_conn
        conn_db = get_conn()
        rows = conn_db.execute(
            "SELECT * FROM trades WHERE agent LIKE 'WarTrader%' AND status='LOSS' "
            "ORDER BY ts DESC LIMIT 100").fetchall()
        conn_db.close()
        trajectories = []
        for row in rows:
            d = dict(row)
            sig = json.loads(d.get("signal_data") or "{}")
            trajectories.append({
                "question": f"Should I have traded this {d['side']} geopolitical signal on {d['symbol']}?",
                "ground_truth": "NO", "agent_answer": "YES",
                "context": {
                    "entry": d["entry_price"], "sl": d["sl_price"], "tp": d["tp_price"],
                    "exit": d["exit_price"], "pnl_pct": d["pnl_pct"],
                    "exchange": sig.get("exchange","unknown"), "risk_type": sig.get("risk_type",""),
                    "severity": sig.get("severity", 0), "signal": sig}})
        from pathlib import Path
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(trajectories, f, indent=2)
        print(f"⚔️ WarTrader: exported {len(trajectories)} failure trajectories -> {output_path}")
        return output_path

    # ── Helper ────────────────────────────────────────────────

    async def _signal_and_execute(self, classification: dict,
                                  venue: ExchangeVenue, event_id: int):
        signal = await self.get_signal(classification, venue)
        if signal:
            await self.execute_signal(signal, venue, event_id)

    # ── Main Loop ─────────────────────────────────────────────

    async def run(self):
        venue_names = [v.name for v in self.venues]
        print(f"⚔️ {AGENT_NAME} started — Exchanges: {venue_names}, Paper: {WAR_PAPER_MODE}")
        scan_count = 0
        while True:
            try:
                print(f"\n🔍 [{AGENT_NAME}] Scanning geopolitical feeds...")
                articles = await self.fetch_geopolitical_news()
                print(f"  [{AGENT_NAME}] {len(articles)} new geopolitical article(s)")
                for article in articles:
                    self._seen_ids.add(article["id"])
                    save_news_event(source=article["source"],
                                    article_id=article["id"], title=article["title"])
                    classification = await self.classify_event(article)
                    if not classification:
                        continue
                    sev, rt = classification.get("severity", 0), classification.get("risk_type", "NEUTRAL")
                    print(f"  ⚔️ GEO EVENT [{sev}/10] {rt} — {article['title'][:60]}")
                    event_id = save_macro_event(
                        event_name=article["title"][:200], event_type=rt, severity=sev,
                        details=json.dumps({"reasoning": classification.get("reasoning",""),
                                            "source": article["source"],
                                            "url": article.get("url",""),
                                            "assets": classification.get("assets",[])}))
                    tasks = [self._signal_and_execute(classification, v, event_id)
                             for v in self.venues]
                    await asyncio.gather(*tasks, return_exceptions=True)
                await self.manage_open_trades()
                scan_count += 1
                if scan_count % 20 == 0:
                    await self.report_performance()
                if scan_count % 100 == 0:
                    self.export_failure_trajectories()
            except Exception as e:
                print(f"WarTrader error: {e}")
                log_issue("HIGH", "AGENT_ERROR", "WarTrader runtime error", str(e))
            await asyncio.sleep(WAR_CHECK_SEC)
