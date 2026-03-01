"""
Layer 10 — Optional AI Assist (Future)

AI may:
  1. Suggest schema mappings
  2. Suggest grouping classification
  3. Flag anomalies
  4. Suggest derived features

AI may NOT execute transformations without explicit confirmation.
All AI actions are suggestions that require human approval.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

# Try to import httpx for API calls
try:
    import httpx

    _HAS_HTTPX = True
except ImportError:
    _HAS_HTTPX = False
    logger.debug("httpx not available — AI assist disabled.")


# The canonical categories the LLM can suggest
CANONICAL_CATEGORIES = [
    "contract_id",
    "close_date",
    "vendor",
    "acv",
    "total_value",
    "term_months",
    "currency",
    "quantity",
    "unit_price",
    "discount",
    "start_date",
    "renewal_date",
    "end_date",
    "auto_renew",
    "close_year",
    "fiscal_period",
    "account_segment",
    "product",
    "department",
    "owner",
    "status",
    "opportunity_type",
    "notes",
    "category",
]


def suggest_column_mappings(
    unmapped_cols: list[str],
    sample_values: dict[str, list[str]],
    api_key: str | None = None,
) -> dict[str, str]:
    """Use an LLM to SUGGEST classifications for unmapped columns.

    These are SUGGESTIONS ONLY — they must be confirmed by the user.

    Args:
        unmapped_cols: List of column names that couldn't be matched.
        sample_values: Dict mapping column name → list of sample values.
        api_key: OpenAI API key. If None, reads from OPENAI_API_KEY env var.

    Returns:
        Dict mapping column_name → suggested canonical_category.
        Returns empty dict if API is unavailable or fails.
    """
    if not unmapped_cols:
        return {}

    key = api_key or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        logger.info("No OPENAI_API_KEY set — AI column suggestions disabled.")
        return {}

    if not _HAS_HTTPX:
        logger.warning("httpx not installed — AI suggestions disabled.")
        return {}

    # Build prompt
    columns_info = []
    for col in unmapped_cols:
        samples = sample_values.get(col, [])[:5]
        samples_str = (
            ", ".join(f'"{s}"' for s in samples) if samples else "(no samples)"
        )
        columns_info.append(f'  - Column: "{col}" | Samples: [{samples_str}]')

    columns_text = "\n".join(columns_info)

    prompt = f"""You are a data classification expert for SaaS contract data.

Given these unmapped spreadsheet columns with sample values, classify each into
one of these canonical categories:

{json.dumps(CANONICAL_CATEGORIES, indent=2)}

If a column doesn't clearly fit any category, classify it as "SKIP".

Columns to classify:
{columns_text}

Respond with ONLY a JSON object mapping column names to categories, like:
{{"Column Name": "Category", "Another Column": "SKIP"}}

Be conservative — only classify if you're confident. Use "SKIP" liberally."""

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a data classification expert. Respond only with valid JSON.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.0,
                    "max_tokens": 500,
                },
            )

        if resp.status_code != 200:
            logger.warning(
                "OpenAI API returned %d: %s", resp.status_code, resp.text[:200]
            )
            return {}

        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()

        # Parse JSON (handle markdown code blocks)
        if content.startswith("```"):
            content = content.split("\n", 1)[1]
            content = content.rsplit("```", 1)[0]
            content = content.strip()

        result = json.loads(content)

        # Filter to valid suggestions
        valid = {}
        for col, cat in result.items():
            if cat in CANONICAL_CATEGORIES and col in unmapped_cols:
                valid[col] = cat
                logger.info("AI suggested: '%s' → '%s'", col, cat)
            elif cat != "SKIP":
                logger.debug("AI returned invalid category '%s' for '%s'.", cat, col)

        return valid

    except json.JSONDecodeError as e:
        logger.warning("Failed to parse AI response as JSON: %s", e)
        return {}
    except Exception as e:
        logger.warning("AI suggestion request failed: %s", e)
        return {}


def suggest_classification(
    profile: dict,
) -> dict[str, str]:
    """Suggest whether a dataset is contract-level or line-item-level.

    Based on profiling metadata (no LLM required — uses heuristics).
    Returns a suggestion dict with reasoning.
    """
    indicators = profile.get("line_item_indicators", [])

    if not indicators:
        return {
            "suggestion": "contract_level",
            "confidence": "medium",
            "reason": "No line-item indicators found",
        }

    high_dup = [i for i in indicators if i.get("duplicate_ratio", 0) > 0.5]
    if high_dup:
        return {
            "suggestion": "line_item_level",
            "confidence": "high",
            "reason": f"High ID duplication detected ({high_dup[0]['duplicate_ratio']:.0%})",
        }

    return {
        "suggestion": "mixed",
        "confidence": "low",
        "reason": "Moderate duplication detected",
    }
