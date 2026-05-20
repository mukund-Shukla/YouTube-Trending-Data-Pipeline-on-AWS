"""
Glue Job: Silver → Gold (Analytics Aggregations)
──────────────────────────────────────────────────
Reads cleansed statistics and category reference data from the Silver layer,
joins them, and produces four business-level aggregation tables in Gold.

Gold tables are optimised for direct querying via Athena and visualisation
in QuickSight. All heavy transformation and metric derivation was done in
Bronze→Silver — this job is purely aggregation logic.

Gold Tables Produced:
  1. trending_analytics   — Daily trending summaries per region
  2. channel_analytics    — Channel performance and regional rankings
  3. category_analytics   — Category-level breakdowns with view share %
  4. video_velocity        — View/like growth rate across trending snapshots

Incremental Load Strategy:
  - new_dates:         Dates in Silver not yet in Gold → append
  - today_dates:       Today's date (always reprocess for fresh view counts) → overwrite
  - historical dates:  Already in Gold and not today → never touched

  Run 1 (May 18): Silver=[18], Gold=[]       → process [18], append
  Run 2 (May 19): Silver=[18,19], Gold=[18]  → process [19], append
  Run 3 (May 19): Silver=[18,19], Gold=[18,19] → process [19], overwrite (fresher counts)

Sources (Silver):
  s3://silver/youtube/statistics/         → clean_statistics
  s3://silver/youtube/reference_data/     → clean_reference_data

Targets (Gold):
  s3://gold/youtube/trending_analytics/
  s3://gold/youtube/channel_analytics/
  s3://gold/youtube/category_analytics/
  s3://gold/youtube/video_velocity/

Job Parameters:
    --JOB_NAME          — Glue job name (auto-set by Glue)
    --silver_database   — Silver Glue catalog database
    --silver_bucket     — Silver S3 bucket name (needed for LAG full-history read)
    --gold_bucket       — Gold S3 bucket name
    --gold_database     — Gold Glue catalog database
"""

import sys
import boto3
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Job Setup ─────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_database",
    "silver_bucket",
    "gold_bucket",
    "gold_database",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Dynamic partition overwrite — only rewrites partitions that are in the current write.
# Without this, mode("overwrite") would nuke the entire table on every run.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

SILVER_DB     = args["silver_database"]
SILVER_BUCKET = args["silver_bucket"]
GOLD_BUCKET   = args["gold_bucket"]
GOLD_DB       = args["gold_database"]

# TODAY in UTC — used to determine which Gold partition to overwrite vs append
# Always re-process today because Silver has fresher view counts from the latest run
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

logger.info(f"Silver DB    : {SILVER_DB}")
logger.info(f"Silver Bucket: {SILVER_BUCKET}")
logger.info(f"Gold Bucket  : {GOLD_BUCKET}")
logger.info(f"Gold DB      : {GOLD_DB}")
logger.info(f"TODAY (UTC)  : {TODAY}")

# Gold S3 paths
GOLD_TRENDING_PATH  = f"s3://{GOLD_BUCKET}/youtube/trending_analytics/"
GOLD_CHANNEL_PATH   = f"s3://{GOLD_BUCKET}/youtube/channel_analytics/"
GOLD_CATEGORY_PATH  = f"s3://{GOLD_BUCKET}/youtube/category_analytics/"
GOLD_VELOCITY_PATH  = f"s3://{GOLD_BUCKET}/youtube/video_velocity/"


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: Scan Gold S3 to find which trending_date partitions already exist
# ══════════════════════════════════════════════════════════════════════════════
def get_existing_gold_dates(bucket: str, prefix: str) -> set:
    """
    Scans Gold S3 to find existing trending_date= partition values.
    Uses a flat key scan instead of two-level delimiter pagination
    to avoid paginator consumption issues with nested prefixes.
    """
    s3 = boto3.client("s3")
    existing_dates = set()
    paginator = s3.get_paginator("list_objects_v2")

    # Scan all keys under the prefix — no Delimiter so we get full flat listing
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            # Key looks like: youtube/trending_analytics/region=ca/trending_date=2026-05-18/part-00000.parquet
            for part in obj["Key"].split("/"):
                if part.startswith("trending_date="):
                    existing_dates.add(part.replace("trending_date=", ""))

    logger.info(f"Gold existing dates found: {sorted(existing_dates)}")
    return existing_dates


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: Incremental write — overwrite today, append new historical dates
# ══════════════════════════════════════════════════════════════════════════════
def write_gold_table(
    df,
    table_name: str,
    s3_path: str,
    partition_keys: list,
    overwrite_dates: set = None,
) -> int:
    """
    Writes a Gold table with split overwrite/append strategy.

    overwrite_dates: partitions to overwrite (today's date only).
                     These have fresh Silver data so Gold must be refreshed.
    Remaining rows:  new historical dates → appended, never touching existing history.

    Why split writes instead of one write:
      Spark's partitionOverwriteMode=dynamic means mode("overwrite") only
      touches partitions present in the current DataFrame. But we still need
      to separate today (overwrite) from new history (append) because:
        - append on an existing today-partition would DUPLICATE rows
        - overwrite on a new-date partition works fine but so does append
      Cleanest solution: overwrite today explicitly, append everything else.
    """
    total_count = df.count()
    logger.info(f"[{table_name}] Total rows to write: {total_count} → {s3_path}")

    if total_count == 0:
        logger.warn(f"[{table_name}] DataFrame is empty — skipping write.")
        return 0

    if overwrite_dates:
        # Split the DataFrame into two buckets
        overwrite_df = df.filter(
            F.col("trending_date").cast("string").isin(list(overwrite_dates))
        )
        append_df = df.filter(
            ~F.col("trending_date").cast("string").isin(list(overwrite_dates))
        )

        overwrite_count = overwrite_df.count()
        append_count    = append_df.count()
        logger.info(f"[{table_name}] Overwrite (today) rows : {overwrite_count}")
        logger.info(f"[{table_name}] Append (new dates) rows: {append_count}")

        # Overwrite today's partition — replaces stale Gold data with fresh Silver counts
        if overwrite_count > 0:
            overwrite_df.write \
                .format("parquet") \
                .option("compression", "snappy") \
                .mode("overwrite") \
                .partitionBy(*partition_keys) \
                .save(s3_path)
            logger.info(f"[{table_name}] Today's partition overwritten.")

        # Append new historical dates — never touches existing Gold partitions
        if append_count > 0:
            append_df.write \
                .format("parquet") \
                .option("compression", "snappy") \
                .mode("append") \
                .partitionBy(*partition_keys) \
                .save(s3_path)
            logger.info(f"[{table_name}] New historical partitions appended.")

    else:
        # No overwrite_dates means everything is new → safe to append all
        df.write \
            .format("parquet") \
            .option("compression", "snappy") \
            .mode("append") \
            .partitionBy(*partition_keys) \
            .save(s3_path)
        logger.info(f"[{table_name}] All rows appended (no overwrite dates provided).")

    logger.info(f"[{table_name}] Write complete — {total_count} rows.")
    return total_count


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: Read Full Silver Statistics History
# ──────────────────────────────────────────────────────────────────────────────
# CRITICAL: We read the FULL Silver history here, not just today's data.
# This is required for the video_velocity LAG() window computation.
# LAG() needs the previous day's views to compute growth rate.
# If we only load today's Silver data, prev_views is always NULL → velocity empty.
#
# No transformation_ctx here — that would enable Glue job bookmarking which
# would only read new files, breaking the full-history requirement for LAG.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("=" * 60)
logger.info("STEP 1: Reading full Silver statistics history...")
logger.info("=" * 60)

# PERMANENT FIX — read directly from S3, catalog irrelevant
stats_df = spark.read \
    .format("parquet") \
    .load(f"s3://{SILVER_BUCKET}/youtube/statistics/")

stats_total_count = stats_df.count()
logger.info(f"Silver statistics loaded: {stats_total_count} records across all dates")

if stats_total_count == 0:
    raise Exception("clean_statistics is empty — aborting Gold job.")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: Determine Which Dates to Process (Incremental Logic)
# ──────────────────────────────────────────────────────────────────────────────
# Cast trending_date to string for comparison.
# trending_date in Silver is stored as Spark DateType (DATE in Parquet).
# When collected to Python via .collect(), DateType becomes datetime.date objects.
# TODAY is a plain string "2026-05-19".
# Without the cast, TODAY in silver_dates silently returns False every time
# because "2026-05-19" != datetime.date(2026, 5, 19).
# ══════════════════════════════════════════════════════════════════════════════
logger.info("STEP 2: Computing incremental dates...")

# Cast to string BEFORE collect to get plain "YYYY-MM-DD" strings in Python
silver_dates = set(
    str(row["trending_date"])
    for row in stats_df.select(
        F.col("trending_date").cast("string").alias("trending_date")
    ).distinct().collect()
)
logger.info(f"Silver dates available: {sorted(silver_dates)}")

# Scan Gold S3 to find already-processed partitions
# Using trending_analytics as the reference table (all 4 Gold tables share same partitions)
gold_existing_dates = get_existing_gold_dates(GOLD_BUCKET, "youtube/trending_analytics/")
logger.info(f"Gold existing dates   : {sorted(gold_existing_dates)}")

# Dates in Silver that Gold has never seen → append
new_dates = silver_dates - gold_existing_dates

# Today must always be reprocessed — Silver has fresher view counts from the latest run.
# Even if today is already in Gold (Run 3 scenario), we overwrite it.
today_dates = {TODAY} if TODAY in silver_dates else set()

# Union: process new history AND refresh today
dates_to_process = new_dates | today_dates

logger.info(f"New dates (append)    : {sorted(new_dates)}")
logger.info(f"Today dates (overwrite): {sorted(today_dates)}")
logger.info(f"Dates to process      : {sorted(dates_to_process)}")

if not dates_to_process:
    logger.info("No new dates and today not in Silver. Nothing to process. Exiting.")
    job.commit()
    sys.exit(0)

# overwrite_dates = only today (if today is in scope)
# These partitions will be overwritten in Gold; all others will be appended
overwrite_dates = today_dates


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: Read Silver Category Reference + Broadcast Join
# ══════════════════════════════════════════════════════════════════════════════
logger.info("STEP 3: Reading Silver category reference data...")

try:
# PERMANENT FIX
    ref_df = spark.read \
        .format("parquet") \
        .load(f"s3://{SILVER_BUCKET}/youtube/reference_data/")

    category_lookup = ref_df.select(
        F.col("category_id").cast("long"),
        F.col("category_name").cast("string"),
    ).dropDuplicates(["category_id"])

    logger.info(f"Category lookup entries: {category_lookup.count()}")

    # Broadcast join — category_lookup is ~180 rows, no shuffle needed
    stats_df = stats_df.join(
        F.broadcast(category_lookup),
        on="category_id",
        how="left",
    )
    logger.info("Category join complete.")

except Exception as e:
    logger.warn(f"Could not load clean_reference_data: {e}")
    logger.warn("Proceeding without category names — category_name will be 'Unknown'")

# Guarantee category_name column exists regardless of join outcome
if "category_name" not in stats_df.columns:
    stats_df = stats_df.withColumn("category_name", F.lit("Unknown"))
else:
    stats_df = stats_df.fillna({"category_name": "Unknown"})

# Deduplicate Silver data — guard against duplicate partitions
# Keeps latest record per video + region + date (highest ingestion_timestamp)
dedup_window = Window \
    .partitionBy("video_id", "region", "trending_date") \
    .orderBy(F.col("ingestion_timestamp").desc())

stats_df = stats_df \
    .withColumn("_row_num", F.row_number().over(dedup_window)) \
    .filter(F.col("_row_num") == 1) \
    .drop("_row_num")

logger.info(f"Deduplicated stats_df: {stats_df.count()} records (full history)")


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4: Filter to Only dates_to_process for trending/channel/category
# ──────────────────────────────────────────────────────────────────────────────
# trending_analytics, channel_analytics, category_analytics only need the
# dates we're processing this run — they don't need full history.
#
# Cast trending_date to string for isin() comparison — same type mismatch
# reason as above: DateType vs string literals in dates_to_process set.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("STEP 4: Filtering stats to dates_to_process...")

stats_df_incremental = stats_df.filter(
    F.col("trending_date").cast("string").isin(list(dates_to_process))
)
incremental_count = stats_df_incremental.count()
logger.info(f"Incremental stats records: {incremental_count} (for dates: {sorted(dates_to_process)})")

if incremental_count == 0:
    logger.warn("No records found for dates_to_process. Check Silver data. Exiting.")
    job.commit()
    sys.exit(0)


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 1: trending_analytics
# ──────────────────────────────────────────────────────────────────────────────
# Daily snapshot of trending video metrics aggregated per region.
# Answers: "How did trending content perform in the US on 2026-05-18?"
# Partitioned by region + trending_date for efficient per-date queries.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("=" * 60)
logger.info("Building Gold Table 1: trending_analytics...")

trending = stats_df_incremental.groupBy("region", "trending_date").agg(
    F.count("video_id").alias("total_videos"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("views").alias("avg_views_per_video"),
    F.max("views").alias("max_views"),
    F.avg("like_ratio").alias("avg_like_ratio"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.countDistinct("channel_title").alias("unique_channels"),
    F.countDistinct("category_id").alias("unique_categories"),
    F.round(
        F.sum(F.when(F.col("definition") == "hd", 1).otherwise(0)) /
        F.count("video_id") * 100, 2
    ).alias("hd_percentage"),
    F.sum(F.when(F.col("views_tier") == "mega",     1).otherwise(0)).alias("mega_count"),
    F.sum(F.when(F.col("views_tier") == "viral",    1).otherwise(0)).alias("viral_count"),
    F.sum(F.when(F.col("views_tier") == "popular",  1).otherwise(0)).alias("popular_count"),
    F.sum(F.when(F.col("views_tier") == "trending", 1).otherwise(0)).alias("trending_count"),
    F.sum(F.when(F.col("views_tier") == "emerging", 1).otherwise(0)).alias("emerging_count"),
)
trending = trending.withColumn("_aggregated_at", F.current_timestamp())

write_gold_table(
    trending, "trending_analytics", GOLD_TRENDING_PATH,
    ["region", "trending_date"], overwrite_dates
)


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 2: channel_analytics
# ──────────────────────────────────────────────────────────────────────────────
# Channel-level performance metrics with regional ranking.
# Answers: "Which channels dominate trending in India?"
# rank_in_region: ranked by total_views within each region on that date.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold Table 2: channel_analytics...")

channel = stats_df_incremental.groupBy("channel_title", "region", "trending_date").agg(
    F.countDistinct("video_id").alias("total_videos"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("views").alias("avg_views_per_video"),
    F.max("views").alias("peak_views"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.avg("like_ratio").alias("avg_like_ratio"),
    F.countDistinct("trending_date").alias("times_trending"),
    F.min("trending_date").alias("first_trending_date"),
    F.max("trending_date").alias("last_trending_date"),
    F.collect_set("category_name").alias("categories"),
    F.first("views_tier").alias("primary_views_tier"),
)

rank_window = Window.partitionBy("region", "trending_date").orderBy(F.col("total_views").desc())
channel = channel.withColumn("rank_in_region", F.row_number().over(rank_window))
channel = channel.withColumn("_aggregated_at", F.current_timestamp())

write_gold_table(
    channel, "channel_analytics", GOLD_CHANNEL_PATH,
    ["region", "trending_date"], overwrite_dates
)


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 3: category_analytics
# ──────────────────────────────────────────────────────────────────────────────
# Category performance over time, per region.
# Answers: "What % of US trending views does Music own today?"
# view_share_pct: each category's % of total views for that region on that date.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold Table 3: category_analytics...")

category = stats_df_incremental.groupBy(
    "category_name", "category_id", "region", "trending_date"
).agg(
    F.count("video_id").alias("video_count"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.avg("like_ratio").alias("avg_like_ratio"),
    F.countDistinct("channel_title").alias("unique_channels"),
    F.max("views").alias("top_video_views"),
    F.round(
        F.sum(F.when(F.col("definition") == "hd", 1).otherwise(0)) /
        F.count("video_id") * 100, 2
    ).alias("hd_percentage"),
)

view_share_window = Window.partitionBy("region", "trending_date")
category = category.withColumn(
    "view_share_pct",
    F.round(
        F.col("total_views") / F.sum("total_views").over(view_share_window) * 100, 2
    )
)

cat_rank_window = Window \
    .partitionBy("region", "trending_date") \
    .orderBy(F.col("total_views").desc())
category = category.withColumn("rank_on_day", F.row_number().over(cat_rank_window))
category = category.withColumn("_aggregated_at", F.current_timestamp())

write_gold_table(
    category, "category_analytics", GOLD_CATEGORY_PATH,
    ["region", "trending_date"], overwrite_dates
)


# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 4: video_velocity
# ──────────────────────────────────────────────────────────────────────────────
# Measures view and like growth for videos trending across multiple days.
#
# CRITICAL — LAG runs on FULL stats_df (not incremental):
#   LAG() looks back at the previous row in the window.
#   If we only pass today's data, there is no previous row → prev_views = NULL
#   → the entire velocity table is empty.
#   So: compute LAG on full Silver history, THEN filter to dates_to_process.
#
# Partitioned by region + trending_date.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold Table 4: video_velocity...")
logger.info("  LAG window running on full Silver history (required for prev_views)...")

daily_snapshot = stats_df.select(
    "video_id", "title", "channel_title", "category_name",
    "region", "trending_date", "views", "likes",
    "comment_count", "engagement_rate", "views_tier",
)

# LAG over full history — partitioned by video+region, ordered by trending_date
velocity_window = Window \
    .partitionBy("video_id", "region") \
    .orderBy("trending_date")

velocity = daily_snapshot \
    .withColumn("prev_views",        F.lag("views").over(velocity_window)) \
    .withColumn("prev_likes",        F.lag("likes").over(velocity_window)) \
    .withColumn("prev_trending_date", F.lag("trending_date").over(velocity_window))

# Exclude first appearance — no previous day to compare against
velocity = velocity.filter(F.col("prev_views").isNotNull())

# Compute deltas
velocity = velocity \
    .withColumn("view_delta",     F.col("views") - F.col("prev_views")) \
    .withColumn("like_delta",     F.col("likes") - F.col("prev_likes")) \
    .withColumn("days_since_prev",
        F.datediff(F.col("trending_date"), F.col("prev_trending_date"))
    )

# Views growth rate % — guarded against division by zero
velocity = velocity.withColumn(
    "view_growth_pct",
    F.when(
        F.col("prev_views") > 0,
        F.round((F.col("views") - F.col("prev_views")) / F.col("prev_views") * 100, 2)
    ).otherwise(F.lit(0.0))
)

# Daily view velocity — views gained per day since last trending appearance
velocity = velocity.withColumn(
    "views_per_day",
    F.when(
        F.col("days_since_prev") > 0,
        F.round(F.col("view_delta") / F.col("days_since_prev"), 0)
    ).otherwise(F.col("view_delta"))
)

# Momentum label
velocity = velocity.withColumn(
    "momentum",
    F.when(F.col("view_growth_pct") >= 20,  F.lit("surging"))
     .when(F.col("view_growth_pct") >= 5,   F.lit("growing"))
     .when(F.col("view_growth_pct") >= -5,  F.lit("stable"))
     .when(F.col("view_growth_pct") >= -20, F.lit("declining"))
     .otherwise(F.lit("fading"))
)

velocity = velocity.withColumn("_aggregated_at", F.current_timestamp())

velocity = velocity.select(
    "video_id", "title", "channel_title", "category_name",
    "region", "trending_date", "prev_trending_date", "days_since_prev",
    "views", "prev_views", "view_delta", "view_growth_pct", "views_per_day",
    "likes", "prev_likes", "like_delta",
    "engagement_rate", "views_tier", "momentum", "_aggregated_at",
)

# NOW filter to dates_to_process — after LAG so prev_views is populated correctly
velocity_incremental = velocity.filter(
    F.col("trending_date").cast("string").isin(list(dates_to_process))
)
velocity_count = velocity_incremental.count()
logger.info(f"  video_velocity incremental rows: {velocity_count}")

if velocity_count == 0:
    logger.warn("  video_velocity is empty — videos need 2+ trending days for LAG to produce rows.")
    logger.warn("  This is expected on Day 1. Run the pipeline again tomorrow.")
else:
    write_gold_table(
        velocity_incremental, "video_velocity", GOLD_VELOCITY_PATH,
        ["region", "trending_date"], overwrite_dates
    )


# ── Summary ───────────────────────────────────────────────────────────────────
logger.info("=" * 60)
logger.info("Gold layer incremental build complete.")
logger.info(f"  Dates processed (new + today) : {sorted(dates_to_process)}")
logger.info(f"  Dates overwritten (today)     : {sorted(overwrite_dates)}")
logger.info(f"  Dates appended (new history)  : {sorted(new_dates)}")
logger.info(f"  Gold tables written to        : s3://{GOLD_BUCKET}/youtube/")
logger.info("  Tables: trending_analytics | channel_analytics | category_analytics | video_velocity")
logger.info("=" * 60)
logger.info("REMINDER: Run MSCK REPAIR TABLE in Athena if trending_date shows NULL:")
logger.info("  MSCK REPAIR TABLE yt_pipeline_gold_prod.trending_analytics;")
logger.info("  MSCK REPAIR TABLE yt_pipeline_gold_prod.channel_analytics;")
logger.info("  MSCK REPAIR TABLE yt_pipeline_gold_prod.category_analytics;")
logger.info("  MSCK REPAIR TABLE yt_pipeline_gold_prod.video_velocity;")

job.commit()