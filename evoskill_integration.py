"""
EvoSkill Integration
Connects BreakoutBot failure trajectories → EvoSkill self-improvement loop
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime
from config import (
    ANTHROPIC_API_KEY,
    EVOSKILL_MAX_ITERATIONS,
    EVOSKILL_FRONTIER_SIZE
)

# ── Scoring Function for EvoSkill ─────────────────────────────

def breakout_scorer(question: str, predicted: str, ground_truth: str) -> float:
    """
    Score agent's trade decision against actual outcome.
    predicted:    agent's answer (YES/NO)
    ground_truth: actual outcome (YES=profitable, NO=loss)
    Returns 1.0 if correct, 0.0 if wrong
    """
    pred = predicted.strip().upper()
    gt   = ground_truth.strip().upper()
    # Partial credit for borderline cases
    if pred == gt:
        return 1.0
    return 0.0


# ── Skills that EvoSkill can discover/improve ─────────────────

INITIAL_SKILLS = {
    "volume_confirmation": """
# Volume Confirmation Skill
When evaluating a breakout signal, check:
- Volume must be at least 1.5x the 20-period average
- Volume should be increasing over the last 3 candles
- Reject signals where volume spikes only on the breakout candle
""",

    "trend_alignment": """
# Trend Alignment Skill  
Before taking a breakout:
- Check 50 EMA direction on current timeframe
- Only take BUY breakouts if price is above 50 EMA
- Only take SELL breakouts if price is below 50 EMA
- Avoid breakouts in ranging markets (EMA flat for 10+ bars)
""",

    "time_filter": """
# Time Filter Skill
Avoid trading during:
- Low liquidity hours (00:00-04:00 UTC)
- Within 30 minutes of major economic events
- Weekend sessions (lower volume, higher spreads)
Best breakouts occur: 08:00-12:00 UTC and 14:00-17:00 UTC
""",

    "false_breakout_filter": """
# False Breakout Filter Skill
Red flags that indicate a false breakout:
- Price closes back inside the range within 2 candles → reject
- Spread unusually wide at time of breakout → reject  
- Breakout against strong HTF support/resistance → reduce size 50%
- News-driven spike without sustained volume → reject
"""
}


def write_initial_skills(skills_dir: str = ".claude/skills"):
    """Write initial skill files for EvoSkill to iterate on"""
    Path(skills_dir).mkdir(parents=True, exist_ok=True)
    for name, content in INITIAL_SKILLS.items():
        path = Path(skills_dir) / f"{name}.md"
        if not path.exists():
            path.write_text(content)
            print(f"📝 Wrote skill: {name}.md")


# ── EvoSkill Task Registration ────────────────────────────────

def make_breakout_agent_options(model: str = "sonnet"):
    """
    Factory function for EvoSkill agent options
    Returns agent config for BreakoutBot decision-making
    """
    # This integrates with EvoSkill's AgentOptions structure
    # EvoSkill will use this to create the base agent
    return {
        "model":       f"claude-{model}-4-5-20250514",
        "system_prompt": Path(".claude/breakout_prompt.txt").read_text()
                         if Path(".claude/breakout_prompt.txt").exists()
                         else BREAKOUT_SYSTEM_PROMPT,
        "skills_dir":  ".claude/skills",
        "task":        "breakout_decision"
    }


BREAKOUT_SYSTEM_PROMPT = """
You are BreakoutBot, a technical analysis trading agent for Bulk perpetuals exchange.

Your job: Given market data and a potential breakout signal, decide YES (take the trade) 
or NO (skip the trade).

Analyze:
1. Breakout direction and strength
2. Volume confirmation
3. Trend alignment
4. Risk/reward ratio
5. Market conditions

Respond with: YES or NO, followed by one line of reasoning.
"""


# ── EvoSkill Runner ───────────────────────────────────────────

def news_trader_scorer(question: str, predicted: str, ground_truth: str) -> float:
    """Score NewsTrader trade decision against actual outcome."""
    pred = predicted.strip().upper()
    gt   = ground_truth.strip().upper()
    return 1.0 if pred == gt else 0.0


NEWS_TRADER_SYSTEM_PROMPT = """
You are NewsTrader, an LLM-powered news trading agent for Bulk perpetuals exchange.

Your job: Given a crypto news article and market context, decide YES (trade it)
or NO (skip it).

Analyze:
1. News sentiment and direction (bullish or bearish catalyst)
2. Impact magnitude (how market-moving is this event?)
3. Asset relevance (is the affected asset in our symbol list?)
4. Timing (is this breaking news or already priced in?)
5. Risk factors (fake news, thin liquidity, countertrend?)

Respond with: YES or NO, followed by one line of reasoning.
"""

INITIAL_NEWS_SKILLS = {
    "impact_filter": """
# Impact Filter Skill
When evaluating a news trade signal:
- Only trade events with clear, immediate market impact
- Avoid trading routine partnership announcements, product updates, or ecosystem grants
- High-impact events: exchange hacks, regulatory bans, ETF approvals, large liquidations
- Very high-impact: exchange insolvency, blanket crypto bans, major protocol exploits
""",

    "freshness_filter": """
# Freshness Filter Skill
Avoid news that is already priced in:
- If the article was published > 15 minutes ago, the market has likely reacted
- If the same news appeared on multiple sources > 5 min apart, skip
- Only trade first-mover articles from primary sources (CoinDesk, CoinTelegraph, TheBlock)
- Breaking news from CryptoPanic hot filter is usually fresh enough
""",

    "direction_clarity": """
# Direction Clarity Skill
Only trade when the directional impact is unambiguous:
- BULLISH signals: ETF approval, institutional buy announcement, protocol upgrade, exchange listing
- BEARISH signals: hack/exploit, regulatory ban, exchange insolvency, major sell-off news
- SKIP when: news is mixed (e.g. regulation with positive and negative aspects), vague ("crypto rally possible")
- When in doubt, skip — there will be a clearer signal next time
""",
}


def write_news_trader_skills(skills_dir: str = ".claude/skills"):
    """Write initial NewsTrader skill files for EvoSkill to iterate on."""
    Path(skills_dir).mkdir(parents=True, exist_ok=True)
    for name, content in INITIAL_NEWS_SKILLS.items():
        path = Path(skills_dir) / f"news_{name}.md"
        if not path.exists():
            path.write_text(content)
            print(f"📝 Wrote news skill: news_{name}.md")


async def run_evoskill_loop(failures_path: str = "data/failures.json"):
    """
    Run EvoSkill self-improvement loop using BreakoutBot failure trajectories
    Call this periodically (e.g., after accumulating 50+ failures)
    """
    if not Path(failures_path).exists():
        print("No failure trajectories found — run BreakoutBot first")
        return

    with open(failures_path) as f:
        trajectories = json.load(f)

    if len(trajectories) < 10:
        print(f"Only {len(trajectories)} failures — need at least 10 to run EvoSkill")
        return

    print(f"🧬 Starting EvoSkill loop with {len(trajectories)} failure trajectories")

    # Write initial skills if not present
    write_initial_skills()
    write_news_trader_skills()
    write_all_agent_skills()

    try:
        # Import EvoSkill (must be installed separately)
        from src.api import EvoSkill, TaskConfig, register_task

        register_task(TaskConfig(
            name="breakout_decision",
            make_agent_options=make_breakout_agent_options,
            scorer=breakout_scorer,
            default_dataset=failures_path,
        ))

        evo = EvoSkill(
            task           = "breakout_decision",
            model          = "sonnet",
            mode           = "skill_only",
            max_iterations = EVOSKILL_MAX_ITERATIONS,
            frontier_size  = EVOSKILL_FRONTIER_SIZE,
        )

        result = await evo.run()

        print(f"✅ EvoSkill loop complete")
        print(f"   Best accuracy: {result.get('best_accuracy', 'N/A')}")
        print(f"   Iterations: {result.get('iterations', 'N/A')}")

        # Save evolved skills summary
        summary_path = f"data/evoskill_result_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.json"
        Path("data").mkdir(exist_ok=True)
        with open(summary_path, "w") as f:
            json.dump(result, f, indent=2)

        return result

    except ImportError:
        print("⚠️ EvoSkill not installed. Clone https://github.com/sentient-agi/EvoSkill")
        print("   Running manual skill analysis instead...")
        await manual_skill_analysis(trajectories)


# ── FundingArb EvoSkill ───────────────────────────────────────

def funding_arb_scorer(question: str, predicted: str, ground_truth: str) -> float:
    pred = predicted.strip().upper()
    gt = ground_truth.strip().upper()
    return 1.0 if pred == gt else 0.0

FUNDING_ARB_SYSTEM_PROMPT = """
You are FundingArb, a funding rate arbitrage agent for Bulk and Hyperliquid exchanges.

Your job: Given funding rate differentials and market conditions, decide YES (open arb)
or NO (skip) for a delta-neutral funding capture opportunity.

Analyze:
1. Funding rate differential magnitude and persistence
2. Market volatility (high vol = more risk for delta-neutral)
3. Spread/slippage cost vs expected funding income
4. Duration of expected rate differential

Respond with: YES or NO, followed by one line of reasoning.
"""

INITIAL_FUNDING_SKILLS = {
    "diff_persistence": """
# Diff Persistence Skill
Only open arb when funding differential is likely to persist:
- Check if the rate difference has been stable for at least 2 funding periods
- Avoid one-time spikes caused by liquidation cascades
- Prefer structural diffs (e.g., exchange congestion) over transient diffs
""",
    "cost_analysis": """
# Cost Analysis Skill
Account for all costs before opening arb:
- Entry slippage on both legs (estimate 2-5 bps per leg)
- Maker/taker fees on both exchanges
- Exit slippage when closing
- Total cost must be < expected funding income over hold period
""",
}


# ── HLCopier EvoSkill ────────────────────────────────────────

def copier_scorer(question: str, predicted: str, ground_truth: str) -> float:
    pred = predicted.strip().upper()
    gt = ground_truth.strip().upper()
    return 1.0 if pred == gt else 0.0

COPIER_SYSTEM_PROMPT = """
You are HLCopier, a whale copy-trading agent for Hyperliquid.

Your job: Given a whale's new position, decide YES (copy) or NO (skip).

Analyze:
1. Whale's historical win rate and PnL
2. Position size relative to their portfolio
3. Market conditions and timing
4. Whether the move is a hedge or a directional bet

Respond with: YES or NO, followed by one line of reasoning.
"""

INITIAL_COPIER_SKILLS = {
    "whale_quality": """
# Whale Quality Skill
Not all whale trades should be copied:
- Verify whale has a positive track record (check account value trend)
- Large positions might be hedges, not directional bets — check for offsetting positions
- Prefer wallets with consistent profits over those with one lucky trade
""",
}


# ── MacroTrader EvoSkill ─────────────────────────────────────

def macro_scorer(question: str, predicted: str, ground_truth: str) -> float:
    pred = predicted.strip().upper()
    gt = ground_truth.strip().upper()
    return 1.0 if pred == gt else 0.0

MACRO_TRADER_SYSTEM_PROMPT = """
You are MacroTrader, an economic calendar trading agent for crypto markets.

Your job: Given an upcoming macro event, decide YES (position before it)
or NO (stay flat).

Analyze:
1. Event impact level and historical market reaction
2. Current positioning and crowding
3. Consensus expectations vs likely surprise direction
4. Crypto correlation to traditional macro events

Respond with: YES or NO, followed by one line of reasoning.
"""

INITIAL_MACRO_SKILLS = {
    "event_playbook": """
# Event Playbook Skill
Standard crypto reactions to macro events:
- FOMC dovish (rate cut/pause): BUY BTC — risk-on, dollar weakens
- FOMC hawkish (rate hike/signal): SELL BTC — risk-off, dollar strengthens
- CPI below expectations: BUY — rate cut expectations increase
- CPI above expectations: SELL — tightening fears
- NFP strong: ambiguous — can be risk-on (economy good) or risk-off (more hikes)
- NFP weak: BUY if market sees rate cuts coming, SELL if recession fears dominate
""",
}


# ── WarTrader EvoSkill ───────────────────────────────────────

def war_scorer(question: str, predicted: str, ground_truth: str) -> float:
    pred = predicted.strip().upper()
    gt = ground_truth.strip().upper()
    return 1.0 if pred == gt else 0.0

WAR_TRADER_SYSTEM_PROMPT = """
You are WarTrader, a geopolitical event trading agent for crypto markets.

Your job: Given a geopolitical event, decide YES (trade it) or NO (skip).

Analyze:
1. Severity and potential for escalation
2. Direct crypto market impact (sanctions, mining bans, etc.)
3. Safe-haven demand dynamics (BTC as digital gold)
4. Whether the event is already priced in

Respond with: YES or NO, followed by one line of reasoning.
"""

INITIAL_WAR_SKILLS = {
    "escalation_filter": """
# Escalation Filter Skill
Only trade geopolitical events with clear escalation trajectory:
- New military action or mobilization → likely to persist, trade it
- Diplomatic statement or "concern" from officials → likely noise, skip
- Sanctions package → trade only if targeting major economy or crypto specifically
- De-escalation or ceasefire → strong risk-on signal, BUY
- Avoid trading on rumors — wait for confirmation from 2+ credible sources
""",
}


def write_all_agent_skills(skills_dir: str = ".claude/skills"):
    """Write initial skills for all agents."""
    Path(skills_dir).mkdir(parents=True, exist_ok=True)
    all_skills = {
        **{f"funding_{k}": v for k, v in INITIAL_FUNDING_SKILLS.items()},
        **{f"copier_{k}": v for k, v in INITIAL_COPIER_SKILLS.items()},
        **{f"macro_{k}": v for k, v in INITIAL_MACRO_SKILLS.items()},
        **{f"war_{k}": v for k, v in INITIAL_WAR_SKILLS.items()},
    }
    for name, content in all_skills.items():
        path = Path(skills_dir) / f"{name}.md"
        if not path.exists():
            path.write_text(content)
            print(f"📝 Wrote skill: {name}.md")


async def manual_skill_analysis(trajectories: list):
    """
    Fallback: Use Claude API directly to analyze failures
    and suggest skill improvements
    """
    if not ANTHROPIC_API_KEY:
        print("ANTHROPIC_API_KEY not set")
        return

    import aiohttp

    # Sample 10 failures
    sample = trajectories[:10]
    failures_text = json.dumps(sample, indent=2)

    prompt = f"""
You are analyzing failed BreakoutBot trades to improve its strategy.

Here are {len(sample)} losing trades with their signal data:

{failures_text}

Analyze the patterns in these losses and suggest:
1. What conditions are common in losing trades?
2. What filters should be added to avoid these?
3. Suggest a new skill file content that would prevent these losses.

Format your response as:
PATTERN: [common pattern]
FILTER: [suggested filter]
SKILL: [skill name]
SKILL_CONTENT: [full skill file content in markdown]
"""

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json",
                     "x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01"},
            json={
                "model":      "claude-sonnet-4-5-20250514",
                "max_tokens": 2000,
                "messages":   [{"role": "user", "content": prompt}]
            }
        ) as resp:
            data = await resp.json()
            analysis = data["content"][0]["text"]
            print("\n🧠 EvoSkill Manual Analysis:\n")
            print(analysis)

            # Save analysis
            Path("data").mkdir(exist_ok=True)
            ts = datetime.utcnow().strftime('%Y%m%d_%H%M')
            with open(f"data/skill_analysis_{ts}.txt", "w") as f:
                f.write(analysis)

            return analysis
