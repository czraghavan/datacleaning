"""
llm_mapper.py — LLM-powered column detection using OpenAI.

Uses gpt-4o-mini to classify unmapped columns based on their name
and sample values. Falls back gracefully if no API key or API error.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

# Try to import httpx (transitive dep from FastAPI/uvicorn)
try:
    import httpx

    _HAS_HTTPX = True
except ImportError:
    _HAS_HTTPX = False
    logger.debug("httpx not available — LLM column detection disabled.")


# The canonical categories the LLM can choose from
CANONICAL_CATEGORIES = [
    "Contract_ID",
    "Contract_Close_Date",
    "Vendor",
    "ACV",
    "Total_Value",
    "Term_Months",
    "Currency",
    "Quantity",
    "Unit_Price",
    "Discount",
    "Effective_Date",
    "Renewal_Date",
    "Expiry_Date",
    "Auto_Renew",
    "Close_Year",
    "Fiscal_Period",
    "Account_Segment",
    "Product",
    "Department",
    "Owner",
    "Status",
    "Opportunity_Type",
    "Notes",
    "Category",
]


def llm_detect_columns(
    unmapped_cols: list[str],
    sample_values: dict[str, list[str]],
    api_key: str | None = None,
) -> dict[str, str]:
    """Use an LLM to classify unmapped columns into canonical categories.

    Args:
        unmapped_cols: List of column names that couldn't be matched.
        sample_values: Dict mapping column name → list of sample values.
        api_key: OpenAI API key. If None, reads from OPENAI_API_KEY env var.

    Returns:
        Dict mapping column_name → canonical_category for confident matches.
        Returns empty dict if API is unavailable or fails.
    """
    if not unmapped_cols:
        return {}

    # Resolve API key
    key = api_key or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        logger.info("No OPENAI_API_KEY set — skipping LLM column detection.")
        return {}

    if not _HAS_HTTPX:
        logger.warning("httpx not installed — skipping LLM column detection.")
        return {}

    # Build the prompt
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

        # Parse the JSON response (handle markdown code blocks)
        if content.startswith("```"):
            content = content.split("\n", 1)[1]  # Remove opening ```json
            content = content.rsplit("```", 1)[0]  # Remove closing ```
            content = content.strip()

        result = json.loads(content)

        # Filter out SKIP and invalid categories
        valid = {}
        for col, cat in result.items():
            if cat in CANONICAL_CATEGORIES and col in unmapped_cols:
                valid[col] = cat
                logger.info("LLM classified: '%s' → '%s'", col, cat)
            elif cat != "SKIP":
                logger.debug(
                    "LLM returned invalid category '%s' for '%s' — skipping.", cat, col
                )

        return valid

    except json.JSONDecodeError as e:
        logger.warning("Failed to parse LLM response as JSON: %s", e)
        return {}
    except httpx.HTTPError as e:
        logger.warning("OpenAI API request failed: %s", e)
        return {}
    except Exception as e:
        logger.warning("Unexpected error in LLM column detection: %s", e)
        return {}
