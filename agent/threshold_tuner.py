"""
Adaptive threshold tuning agent for the fraud detection pipeline.

Reads alert metrics from Redis, uses Claude to reason over them, and proposes
threshold adjustments. Run this script to review and accept/reject proposals;
accepted changes are written to Redis and picked up by the worker immediately.

Usage:
    python agent/threshold_tuner.py

Environment variables:
    ANTHROPIC_API_KEY  (required)
    REDIS_HOST         (default: 127.0.0.1)
"""

import os
import sys
import json
import redis
import anthropic

from dotenv import load_dotenv
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")

THRESHOLD_DEFAULTS = {
    "velocity_limit": 5,
    "geo_distance_km": 500,
    "spike_multiplier": 5,
}

TOOLS = [
    {
        "name": "get_fraud_metrics",
        "description": (
            "Read the current fraud detection thresholds and alert counts from Redis. "
            "Returns thresholds (current values), alert_counts per rule type, and tx_total "
            "(total transactions processed since counters were last reset)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "propose_threshold_change",
        "description": (
            "Propose an adjustment to a single fraud detection threshold. "
            "Call this once per threshold you want to change. "
            "Do not call it if the data does not justify a change."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "threshold": {
                    "type": "string",
                    "enum": ["velocity_limit", "geo_distance_km", "spike_multiplier"],
                    "description": (
                        "velocity_limit: max transactions per 60s before alert. "
                        "geo_distance_km: max km between consecutive transactions. "
                        "spike_multiplier: multiplier over average spend to trigger alert."
                    ),
                },
                "current_value": {
                    "type": "number",
                    "description": "The value currently in effect.",
                },
                "proposed_value": {
                    "type": "number",
                    "description": "The value you are recommending.",
                },
                "rationale": {
                    "type": "string",
                    "description": "Concise explanation grounded in the metric data.",
                },
            },
            "required": ["threshold", "current_value", "proposed_value", "rationale"],
        },
    },
]

SYSTEM_PROMPT = """\
You are a fraud detection threshold tuning agent. Your job is to analyze alert
rate data and propose precise, data-driven adjustments to three thresholds:

  velocity_limit    — max transactions per 60 s before a velocity alert fires
  geo_distance_km   — max km between consecutive transactions before a travel alert fires
  spike_multiplier  — multiplier over a user's average spend to trigger a spike alert

Reasoning guidelines:
- A rule with a very high alert rate relative to total transactions is probably too
  sensitive (threshold is too tight). Raise it modestly.
- A rule with zero or near-zero alerts despite significant transaction volume may be
  too permissive. Lower it modestly.
- If tx_total is very low (< 50) the data is too sparse to draw conclusions — say so
  and propose no changes.
- Prefer small, conservative adjustments (10–30%) over large leaps.
- Only call propose_threshold_change when the data clearly justifies it.
- Always call get_fraud_metrics first.
"""


def _fetch_metrics(r: redis.Redis) -> dict:
    stored = r.hgetall("fraud:thresholds")
    thresholds = {}
    for key, default in THRESHOLD_DEFAULTS.items():
        raw = stored.get(key, default)
        thresholds[key] = float(raw) if isinstance(default, float) else int(raw)

    alert_counts = {
        "velocity": int(r.get("fraud:alert_count:velocity") or 0),
        "spending_spike": int(r.get("fraud:alert_count:spending_spike") or 0),
        "travel_impossibility": int(r.get("fraud:alert_count:travel_impossibility") or 0),
    }
    tx_total = int(r.get("fraud:tx_total") or 0)

    return {"thresholds": thresholds, "alert_counts": alert_counts, "tx_total": tx_total}


MOCK_PROPOSALS = [
    {
        "threshold": "velocity_limit",
        "current_value": 5,
        "proposed_value": 8,
        "rationale": "[MOCK] velocity alerts are firing on 38% of transactions — threshold is too tight for normal usage patterns. Raising to 8 gives headroom for legitimate bursts.",
    },
    {
        "threshold": "geo_distance_km",
        "current_value": 500,
        "proposed_value": 300,
        "rationale": "[MOCK] travel_impossibility alert rate is very low (< 1%) despite high transaction volume, suggesting the 500 km threshold is too permissive. Lowering to 300 km tightens coverage without over-firing.",
    },
]


def run_mock_agent() -> list[dict]:
    print("(mock mode — skipping Claude API call)\n")
    return MOCK_PROPOSALS


def run_agent(r: redis.Redis) -> list[dict]:
    """Run the Claude tool-use loop and return a list of proposals."""
    client = anthropic.Anthropic()
    proposals: list[dict] = []

    messages = [
        {
            "role": "user",
            "content": (
                "Please analyse the current fraud metrics and propose any threshold "
                "changes that are warranted by the data."
            ),
        }
    ]

    while True:
        response = client.messages.create(
            model="claude-opus-4-8",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            break

        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue

            if block.name == "get_fraud_metrics":
                result = _fetch_metrics(r)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result),
                    }
                )

            elif block.name == "propose_threshold_change":
                proposals.append(block.input)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": "Proposal recorded. Continue with any remaining thresholds.",
                    }
                )

        if tool_results:
            messages.append({"role": "user", "content": tool_results})

    return proposals


def run_cli() -> None:
    mock = "--mock" in sys.argv

    if not mock:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            print("Error: ANTHROPIC_API_KEY environment variable is not set.")
            raise SystemExit(1)

    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    try:
        r.ping()
    except redis.exceptions.ConnectionError:
        print(f"Error: could not connect to Redis at {REDIS_HOST}.")
        raise SystemExit(1)

    print("Running threshold tuning agent...\n")
    proposals = run_mock_agent() if mock else run_agent(r)

    if not proposals:
        print("Agent found no threshold adjustments needed.")
        return

    print(f"Agent proposed {len(proposals)} change(s):\n")

    accepted = 0
    for i, p in enumerate(proposals, 1):
        print(f"Proposal {i}/{len(proposals)}")
        print(f"  Threshold      : {p['threshold']}")
        print(f"  Current value  : {p['current_value']}")
        print(f"  Proposed value : {p['proposed_value']}")
        print(f"  Rationale      : {p['rationale']}")

        while True:
            answer = input("\n  Accept or reject? [a/r]: ").strip().lower()
            if answer in ("a", "accept"):
                r.hset("fraud:thresholds", p["threshold"], p["proposed_value"])
                print(f"  Accepted — fraud:thresholds[{p['threshold']}] = {p['proposed_value']}")
                accepted += 1
                break
            elif answer in ("r", "reject"):
                print("  Rejected.")
                break
            else:
                print("  Please enter 'a' to accept or 'r' to reject.")

        print()

    print(f"Done. {accepted}/{len(proposals)} proposal(s) applied to Redis.")


if __name__ == "__main__":
    run_cli()
