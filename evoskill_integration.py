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
