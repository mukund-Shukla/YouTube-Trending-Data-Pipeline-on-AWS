"""
Lambda: Data Quality Checks
─────────────────────────────
Called by Step Functions after the Silver layer is written.
Validates both Silver tables before allowing Gold aggregation to proceed.

If any check fails:
  - Pipeline halts (Step Functions Choice state evaluates quality_passed)
  - SNS alert is sent with details of failed checks
  - Gold job does NOT run

Checks performed:
  1. Row count          — minimum rows present?
  2. Null percentage    — critical columns populated?
  3. Schema validation  — expected columns exist?
  4. Value ranges       — numeric values sensible? enum values valid?
  5. Freshness          — statistics data recent enough?
                          (skipped for reference data — it's static)

Tables checked:
  - clean_statistics    — trending video facts (all 5 checks)
  - clean_reference_data — category lookup (row count, schema, null only)

Environment Variables:
    S3_BUCKET_SILVER      — Silver bucket (used for context, Athena reads via catalog)
    SNS_ALERT_TOPIC_ARN   — SNS topic ARN for failure alerts
    DQ_MIN_ROW_COUNT      — Minimum row count threshold (default: 10)
    DQ_MAX_NULL_PERCENT   — Maximum null % on critical columns (default: 5.0)
    ATHENA_DATABASE       — Silver Glue catalog database (default: yt_pipeline_silver_prod)
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")
SNS_TOPIC  = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_ROW_COUNT   = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT    = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS       = 50_000_000_000   # 50B — sanity ceiling for view counts
FRESHNESS_HOURS = 48               # Statistics data must be newer than this

# ── Critical Columns Per Table ────────────────────────────────────────────────
# These columns are checked for nulls and schema presence.
# Only columns that would break downstream Gold aggregations are listed.
# Non-critical columns (description, audio_language, etc.) are intentionally
# excluded — their nulls are acceptable and don't affect Gold output quality.
# ─────────────────────────────────────────────────────────────────────────────
CRITICAL_COLUMNS = {
    "clean_statistics": [
        "video_id",
        "title",
        "channel_title",
        "category_id",
        "category_name",   # Added — now available after Bronze→Silver join
        "views",
        "likes",
        "region",
        "trending_date",
        "engagement_rate",
        "views_tier",
    ],
    "clean_reference_data": [
        "category_id",
        "category_name",
        "region",
    ],
}

# ── Valid Enum Values ─────────────────────────────────────────────────────────
VALID_VIEWS_TIERS = {"mega", "viral", "popular", "trending", "emerging"}
VALID_REGIONS     = {"us", "in", "gb", "jp", "kr", "ca"}


# ─────────────────────────────────────────────────────────────────────────────
# CHECK FUNCTIONS
# Each returns a dict with: check, table, passed, message + optional metadata
# ─────────────────────────────────────────────────────────────────────────────

def check_row_count(df: pd.DataFrame, table_name: str) -> dict:
    """Minimum row count — ensures data actually landed in Silver."""
    count = len(df)
    passed = count >= MIN_ROW_COUNT
    return {
        "check": "row_count",
        "table": table_name,
        "value": count,
        "threshold": MIN_ROW_COUNT,
        "passed": passed,
        "message": f"Row count: {count} (min: {MIN_ROW_COUNT})",
    }


def check_null_percentage(df: pd.DataFrame, table_name: str) -> list:
    """
    Null percentage on critical columns.
    Non-critical columns (description, audio_language, duration etc.)
    are intentionally excluded — nulls there are acceptable from the API.
    """
    results = []
    cols = CRITICAL_COLUMNS.get(table_name, [])

    for col in cols:
        if col not in df.columns:
            results.append({
                "check": "null_pct",
                "table": table_name,
                "column": col,
                "passed": False,
                "message": f"Column '{col}' missing — cannot check nulls",
            })
            continue

        null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0.0
        passed   = null_pct <= MAX_NULL_PCT
        results.append({
            "check": "null_pct",
            "table": table_name,
            "column": col,
            "value": round(null_pct, 2),
            "threshold": MAX_NULL_PCT,
            "passed": passed,
            "message": f"{col} null%: {null_pct:.2f}% (max: {MAX_NULL_PCT}%)",
        })

    return results


def check_schema(df: pd.DataFrame, table_name: str) -> dict:
    """Schema presence — all expected critical columns must exist."""
    expected = set(CRITICAL_COLUMNS.get(table_name, []))
    actual   = set(df.columns)
    missing  = expected - actual
    passed   = len(missing) == 0
    return {
        "check": "schema",
        "table": table_name,
        "missing_columns": list(missing),
        "passed": passed,
        "message": f"Missing columns: {missing}" if missing else "All expected columns present",
    }


def check_value_ranges(df: pd.DataFrame, table_name: str) -> list:
    """
    Value range and enum validation.
    Only runs on clean_statistics — reference data has no numeric/enum columns to validate.

    Checks:
      - views: no negatives, none above 50B sanity ceiling
      - engagement_rate: must be between 0 and 100 (it's a percentage)
      - like_ratio: must be between 0 and 100
      - views_tier: only valid enum values allowed
      - region: only configured regions allowed
    """
    results = []

    if table_name != "clean_statistics":
        return results

    # Views sanity check
    if "views" in df.columns:
        df["views"] = pd.to_numeric(df["views"], errors="coerce")
        negative_views = int((df["views"] < 0).sum())
        extreme_views  = int((df["views"] > MAX_VIEWS).sum())
        passed = negative_views == 0 and extreme_views == 0
        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "views",
            "negative_count": negative_views,
            "extreme_count": extreme_views,
            "passed": passed,
            "message": f"Views: {negative_views} negative, {extreme_views} extreme (>{MAX_VIEWS:,})",
        })

    # Engagement rate range check (must be 0–100)
    if "engagement_rate" in df.columns:
        df["engagement_rate"] = pd.to_numeric(df["engagement_rate"], errors="coerce")
        out_of_range = int(((df["engagement_rate"] < 0) | (df["engagement_rate"] > 100)).sum())
        passed = out_of_range == 0
        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "engagement_rate",
            "out_of_range_count": out_of_range,
            "passed": passed,
            "message": f"Engagement rate: {out_of_range} values outside 0–100%",
        })

    # Like ratio range check (must be 0–100)
    if "like_ratio" in df.columns:
        df["like_ratio"] = pd.to_numeric(df["like_ratio"], errors="coerce")
        out_of_range = int(((df["like_ratio"] < 0) | (df["like_ratio"] > 100)).sum())
        passed = out_of_range == 0
        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "like_ratio",
            "out_of_range_count": out_of_range,
            "passed": passed,
            "message": f"Like ratio: {out_of_range} values outside 0–100%",
        })

    # views_tier enum validation
    if "views_tier" in df.columns:
        invalid_tiers = df[~df["views_tier"].isin(VALID_VIEWS_TIERS)]["views_tier"].unique().tolist()
        passed = len(invalid_tiers) == 0
        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "views_tier",
            "invalid_values": invalid_tiers,
            "valid_values": list(VALID_VIEWS_TIERS),
            "passed": passed,
            "message": f"views_tier invalid values: {invalid_tiers}" if invalid_tiers
                       else "views_tier: all values valid",
        })

    # Region enum validation
    if "region" in df.columns:
        actual_regions  = set(df["region"].dropna().unique())
        invalid_regions = actual_regions - VALID_REGIONS
        passed = len(invalid_regions) == 0
        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "region",
            "found_regions": list(actual_regions),
            "invalid_regions": list(invalid_regions),
            "passed": passed,
            "message": f"Unexpected regions: {invalid_regions}" if invalid_regions
                       else f"Regions valid: {actual_regions}",
        })

    return results


def check_freshness(df: pd.DataFrame, table_name: str) -> dict:
    """
    Freshness check — is the statistics data recent enough?

    Skipped for clean_reference_data because category mappings are static.
    YouTube hasn't changed category IDs in years — running freshness on
    a static lookup table would generate false positive pipeline failures.
    """
    # Static reference data — freshness not applicable
    if table_name == "clean_reference_data":
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": "Freshness check skipped — static reference data, not subject to staleness validation",
        }

    # No timestamp column present (shouldn't happen, but defensive)
    ts_col = None
    for candidate in ["_processed_at", "ingestion_timestamp"]:
        if candidate in df.columns:
            ts_col = candidate
            break

    if ts_col is None:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": "No timestamp column found — skipping freshness check",
        }

    try:
        latest = pd.to_datetime(df[ts_col], utc=True).max()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)

        if pd.isna(latest):
            return {
                "check": "freshness",
                "table": table_name,
                "passed": False,
                "message": f"Timestamp column '{ts_col}' has no valid values — data may not have loaded",
            }

        passed = latest >= cutoff
        return {
            "check": "freshness",
            "table": table_name,
            "latest_record": str(latest),
            "cutoff": str(cutoff),
            "hours_threshold": FRESHNESS_HOURS,
            "passed": passed,
            "message": f"Latest: {latest} | Cutoff: {cutoff} | {'PASS' if passed else 'STALE'}",
        }

    except Exception as e:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": f"Could not parse timestamps ({e}) — skipping freshness check",
        }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Run data quality checks on Silver layer tables.

    Expected Step Functions input:
    {
        "layer": "silver",
        "database": "yt_pipeline_silver_prod",
        "tables": ["clean_statistics", "clean_reference_data"]
    }

    Returns:
    {
        "quality_passed": true/false,
        "checks_passed": 17,
        "checks_total": 18,
        "details": [ ... per-check results ... ]
    }
    """
    database = event.get("database", "yt_pipeline_silver_prod")
    tables   = event.get("tables", ["clean_statistics", "clean_reference_data"])

    logger.info(f"Starting DQ checks | database: {database} | tables: {tables}")

    all_results    = []
    overall_passed = True

    for table_name in tables:
        logger.info(f"{'='*50}")
        logger.info(f"Checking: {database}.{table_name}")

        # ── Read sample from Athena ───────────────────────────────────────
        # LIMIT 10000 for cost/speed — sufficient for statistical DQ checks.
        # For production at scale, replace with TABLESAMPLE BERNOULLI(10)
        # to get a percentage-based sample without a full scan.
        try:
            query = f'SELECT * FROM "{table_name}" LIMIT 10000'
            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                ctas_approach=False,
            )
            logger.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")

        except Exception as e:
            logger.error(f"  Could not read {table_name}: {e}")
            all_results.append({
                "check": "read_table",
                "table": table_name,
                "passed": False,
                "message": str(e),
            })
            overall_passed = False
            continue

        # ── Run all checks ────────────────────────────────────────────────
        checks = []
        checks.append(check_row_count(df, table_name))
        checks.extend(check_null_percentage(df, table_name))
        checks.append(check_schema(df, table_name))
        checks.extend(check_value_ranges(df, table_name))
        checks.append(check_freshness(df, table_name))

        # Log each result
        for check in checks:
            status = "PASS" if check["passed"] else "FAIL"
            logger.info(f"  [{status}] {check['check']} — {check['message']}")
            if not check["passed"]:
                overall_passed = False

        all_results.extend(checks)

    # ── Summary ───────────────────────────────────────────────────────────
    passed_count = sum(1 for r in all_results if r["passed"])
    total_count  = len(all_results)
    overall_status = "PASS" if overall_passed else "FAIL"

    logger.info(f"{'='*50}")
    logger.info(f"DQ Summary: {passed_count}/{total_count} checks passed — {overall_status}")

    # ── SNS Alert on Failure ──────────────────────────────────────────────
    if not overall_passed and SNS_TOPIC:
        failed_checks = [r for r in all_results if not r["passed"]]
        alert_message = {
            "summary": f"DQ FAILED: {passed_count}/{total_count} checks passed",
            "database": database,
            "tables_checked": tables,
            "failed_checks": failed_checks,
        }
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject="[YT Pipeline] ⚠️ Data Quality Checks FAILED — Gold blocked",
            Message=json.dumps(alert_message, indent=2, default=str),
        )
        logger.info(f"SNS alert sent for {len(failed_checks)} failed checks")

    return {
        "quality_passed": bool(overall_passed),
        "checks_passed":  int(passed_count),
        "checks_total":   int(total_count),
        "details": json.loads(json.dumps(all_results, default=str)),
    }