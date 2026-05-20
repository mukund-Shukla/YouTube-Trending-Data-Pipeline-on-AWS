"""
Glue Job: Bronze → Silver (YouTube API Statistics)
────────────────────────────────────────────────────
Reads raw JSON trending video data written by the ingestion Lambda
from the Bronze S3 layer, flattens the nested API response structure,
applies schema enforcement, cleansing, deduplication, and derived
metrics — then writes clean Parquet to the Silver layer.

Source format: YouTube Data API v3 JSON response
  s3://bronze/youtube/raw_statistics/region={r}/date={d}/hour={h}/*.json

Target format: Snappy-compressed Parquet, partitioned by region + date
  s3://silver/youtube/statistics/region={r}/date={d}/

Job Parameters:
    --JOB_NAME          — Glue job name (auto-set by Glue)
    --bronze_database   — Bronze Glue catalog database
    --bronze_table      — Bronze statistics catalog table (raw_statistics)
    --bronze_bucket     — Bronze S3 bucket name
    --silver_bucket     — Silver S3 bucket name
    --silver_database   — Silver Glue catalog database
    --silver_table      — Silver statistics catalog table (clean_statistics)
"""

import sys
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, BooleanType
from pyspark.sql.window import Window

# ── Job Setup ─────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_database",
    "bronze_table",
    "bronze_bucket",
    "silver_bucket",
    "silver_database",
    "silver_table",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_DB     = args["bronze_database"]
BRONZE_TABLE  = args["bronze_table"]
BRONZE_BUCKET = args["bronze_bucket"]
SILVER_BUCKET = args["silver_bucket"]
SILVER_DB     = args["silver_database"]
SILVER_TABLE  = args["silver_table"]

BRONZE_PATH          = f"s3://{BRONZE_BUCKET}/youtube/raw_statistics/"
BRONZE_CATEGORY_PATH = f"s3://{BRONZE_BUCKET}/youtube/raw_statistics_reference_data/"
SILVER_PATH          = f"s3://{SILVER_BUCKET}/youtube/statistics/"
SILVER_CATEGORY_PATH = f"s3://{SILVER_BUCKET}/youtube/reference_data/"

REGIONS = ["us", "in", "gb", "jp", "kr", "ca"]

logger.info(f"Bronze path : {BRONZE_PATH}")
logger.info(f"Silver path : {SILVER_PATH}")
logger.info(f"Regions     : {REGIONS}")


# ── Step 1: Read Raw JSON from Bronze (per-region, fault-tolerant) ────────────
# Reading per region rather than from the Glue catalog table because:
#   - Catalog schema reflects the full nested JSON structure which Glue
#     flattens inconsistently across crawler runs
#   - Direct S3 JSON read gives us full control over nested field access
#   - Per-region loop isolates failures — one bad region won't kill the job
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Reading Bronze JSON data per region...")

dfs = []
failed_regions = []


for region in REGIONS:
    region_path = f"{BRONZE_PATH}region={region}/"
    try:
        region_df = spark.read \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .json(region_path)

        if "items" not in region_df.columns:
            logger.warn(f"{region}: No 'items' column found — skipping")
            failed_regions.append(region)
            continue

        # Extract top-level metadata BEFORE exploding items
        # _pipeline_metadata sits at root level, not inside each item
        region_df = region_df.select(
            F.explode("items").alias("item"),
            F.col("_pipeline_metadata.ingestion_timestamp").alias("ingestion_timestamp"),
            F.col("_pipeline_metadata.ingestion_id").alias("ingestion_id"),
        ).withColumn("region", F.lit(region))

        count = region_df.count()
        logger.info(f"{region}: {count} video records loaded")
        dfs.append(region_df)

    except Exception as e:
        logger.error(f"{region}: Failed to read — {str(e)}")
        failed_regions.append(region)

if not dfs:
    raise Exception("All regions failed to load from Bronze. Aborting job.")

if failed_regions:
    logger.warn(f"Regions skipped due to read errors: {failed_regions}")

# Union all successful regions into one DataFrame
raw_df = dfs[0]
for region_df in dfs[1:]:
    raw_df = raw_df.unionByName(region_df, allowMissingColumns=True)

initial_count = raw_df.count()
logger.info(f"Total raw records across all regions: {initial_count}")


# ── Step 2: Flatten Nested API Structure ──────────────────────────────────────
# YouTube API v3 returns nested objects under item.snippet and item.statistics.
# We flatten these into a clean, flat schema.
#
# Note on dislikes: YouTube Data API v3 removed public dislike counts in
# December 2021. The field no longer exists in API responses, so we
# hardcode 0 rather than trying to read a field that will always be null.
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Flattening API JSON structure...")

df = raw_df.select(
    # Core identifiers
    F.col("item.id").cast(StringType()).alias("video_id"),
    F.col("region"),

    # Snippet fields
    F.col("item.snippet.title").cast(StringType()).alias("title"),
    F.col("item.snippet.channelTitle").cast(StringType()).alias("channel_title"),
    F.col("item.snippet.categoryId").cast(LongType()).alias("category_id"),
    F.col("item.snippet.publishedAt").cast(StringType()).alias("publish_time"),
    F.col("item.snippet.description").cast(StringType()).alias("description"),
    F.col("item.snippet.defaultAudioLanguage").cast(StringType()).alias("audio_language"),

    # Statistics fields — all come as strings from the API, cast to Long
    F.col("item.statistics.viewCount").cast(LongType()).alias("views"),
    F.col("item.statistics.likeCount").cast(LongType()).alias("likes"),
    F.lit(0).cast(LongType()).alias("dislikes"),   # Removed from API v3 Dec 2021
    F.col("item.statistics.commentCount").cast(LongType()).alias("comment_count"),
    F.col("item.statistics.favoriteCount").cast(LongType()).alias("favorite_count"),

    # Content details
    F.col("item.contentDetails.duration").cast(StringType()).alias("duration"),
    F.col("item.contentDetails.definition").cast(StringType()).alias("definition"),  # hd / sd

    # Pipeline metadata injected by ingestion Lambda
    F.col("ingestion_timestamp").cast(StringType()).alias("ingestion_timestamp"),
    F.col("ingestion_id").cast(StringType()).alias("ingestion_id"),
)

logger.info(f"Flattened schema: {df.columns}")


# ── Step 3: Data Cleansing ────────────────────────────────────────────────────
logger.info("Cleansing data...")

# Drop rows where video_id is null — these are corrupt/incomplete API responses
before_filter = df.count()
df = df.filter(F.col("video_id").isNotNull() & (F.col("video_id") != ""))
dropped_nulls = before_filter - df.count()
if dropped_nulls > 0:
    logger.warn(f"Dropped {dropped_nulls} rows with null/empty video_id")

# Standardize region to lowercase (already lowercase from ingestion but defensive)
df = df.withColumn("region", F.lower(F.trim(F.col("region"))))

# Parse ingestion_timestamp to proper timestamp type for freshness checks downstream
df = df.withColumn(
    "ingestion_timestamp",
    F.to_timestamp(F.col("ingestion_timestamp"))
)

# Parse publish_time to proper timestamp
df = df.withColumn(
    "publish_time",
    F.to_timestamp(F.col("publish_time"))
)

# Add trending_date derived from ingestion_timestamp
# This is the date the video appeared in trending — used for partitioning and analysis
df = df.withColumn(
    "trending_date",
    F.to_date(F.col("ingestion_timestamp"))
)

# Fill nulls for numeric columns with 0 — null counts cause aggregation issues in Gold
numeric_cols = ["views", "likes", "dislikes", "comment_count", "favorite_count"]
for col_name in numeric_cols:
    df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

# Standardize definition column values
df = df.withColumn(
    "definition",
    F.when(F.col("definition").isin("hd", "sd"), F.col("definition"))
     .otherwise(F.lit("unknown"))
)


# ── Step 4: Derived Metrics ───────────────────────────────────────────────────
# These computed columns are business metrics used directly in Gold aggregations.
# Computing them here (Silver) means Gold job is purely aggregation logic.
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Adding derived metrics...")

df = df.withColumn(
    "like_ratio",
    F.when(
        F.col("views") > 0,
        F.round(F.col("likes") / F.col("views") * 100, 4)
    ).otherwise(F.lit(0.0))
)

df = df.withColumn(
    "engagement_rate",
    F.when(
        F.col("views") > 0,
        F.round(
            (F.col("likes") + F.col("comment_count")) / F.col("views") * 100,
            4
        )
    ).otherwise(F.lit(0.0))
    # Note: dislikes excluded from engagement_rate since API no longer provides them
)

# Views tier — useful for category-level analysis in Gold
df = df.withColumn(
    "views_tier",
    F.when(F.col("views") >= 100_000_000, F.lit("mega"))       # 100M+
     .when(F.col("views") >= 10_000_000,  F.lit("viral"))      # 10M+
     .when(F.col("views") >= 1_000_000,   F.lit("popular"))    # 1M+
     .when(F.col("views") >= 100_000,     F.lit("trending"))   # 100K+
     .otherwise(F.lit("emerging"))
)


# ── Step 5: Deduplication ─────────────────────────────────────────────────────
# The ingestion Lambda runs every 6 hours. The same video can appear in
# multiple runs on the same day (it may stay trending all day).
# We keep the LATEST record per video + region + date to capture
# the most up-to-date view/like counts for that day.
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Deduplicating...")

dedup_window = Window \
    .partitionBy("video_id", "region", "trending_date") \
    .orderBy(F.col("ingestion_timestamp").desc())

df = df.withColumn("_row_num", F.row_number().over(dedup_window)) \
       .filter(F.col("_row_num") == 1) \
       .drop("_row_num")

clean_count = df.count()
logger.info(f"After dedup: {clean_count} records (removed {initial_count - clean_count} duplicates)")


# ── Step 6: Processing Metadata ───────────────────────────────────────────────
df = df.withColumn("_processed_at", F.current_timestamp())
df = df.withColumn("_job_name", F.lit(args["JOB_NAME"]))


# ── Step 7: Data Quality Warnings (non-blocking) ──────────────────────────────
# These are warnings logged to CloudWatch — they do NOT stop the job.
# Blocking DQ checks happen in the dedicated DQ Lambda after this job completes.
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Running pre-write DQ warnings...")

for col_name in ["video_id", "title", "channel_title", "views"]:
    null_count = df.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        logger.warn(f"DQ WARNING: '{col_name}' has {null_count} null values")

negative_views = df.filter(F.col("views") < 0).count()
if negative_views > 0:
    logger.warn(f"DQ WARNING: {negative_views} records with negative view counts")

zero_views = df.filter(F.col("views") == 0).count()
if zero_views > 0:
    logger.warn(f"DQ INFO: {zero_views} records with 0 views (new uploads or restricted)")

logger.info("Pre-write DQ warnings complete.")


# ── Step 8: Write to Silver Layer ─────────────────────────────────────────────
# Partition by region AND trending_date for efficient Athena queries.
# Partition by date means queries like "show me trending for last 7 days"
# scan only the relevant date partitions instead of the entire dataset.
#
# overwrite_partitions mode = idempotent:
# Re-running this job for the same region+date overwrites that partition
# rather than appending duplicate rows.
# ─────────────────────────────────────────────────────────────────────────────
# Remove DynamicFrame conversion + getSink block, replace with:
logger.info(f"Writing {clean_count} records to Silver: {SILVER_PATH}")

df.write \
    .format("parquet") \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .partitionBy("region", "trending_date") \
    .save(SILVER_PATH)

logger.info(f"Silver statistics write complete — {clean_count} records")

logger.info(f"Silver write complete — {clean_count} records written to {SILVER_PATH}")
logger.info(f"Partitioned by: region × trending_date")
logger.info(f"Regions processed: {[r for r in REGIONS if r not in failed_regions]}")


# ═════════════════════════════════════════════════════════════════════════════
# PART 2: Category Reference Data
# ─────────────────────────────────────────────────────────────────────────────
# Reads category JSON files from Bronze reference_data prefix.
# Produces a clean category ID → name lookup table in Silver.
#
# Why consolidated here instead of a separate Lambda:
#   - Both datasets share the same source format (YouTube API v3 JSON)
#   - Single job run keeps both Silver tables in sync
#   - Eliminates a separate Lambda deploy, IAM role, and Step Functions branch
# ═════════════════════════════════════════════════════════════════════════════

logger.info("=" * 60)
logger.info("PART 2: Processing category reference data...")
logger.info("=" * 60)

cat_dfs = []
cat_failed_regions = []

for region in REGIONS:
    cat_region_path = f"{BRONZE_CATEGORY_PATH}region={region}/"
    try:
        cat_df = spark.read \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .json(cat_region_path)

        if "items" not in cat_df.columns:
            logger.warn(f"{region} categories: No 'items' column — skipping. Columns: {cat_df.columns}")
            cat_failed_regions.append(region)
            continue

        # Explode items — each row = one category entry
        # Extract _pipeline_metadata before exploding (same pattern as statistics)
        cat_df = cat_df.select(
            F.explode("items").alias("item"),
            F.col("_pipeline_metadata.ingestion_timestamp").alias("ingestion_timestamp"),
        ).withColumn("region", F.lit(region))

        count = cat_df.count()
        logger.info(f"{region}: {count} category records loaded")
        cat_dfs.append(cat_df)

    except Exception as e:
        logger.error(f"{region} categories: Failed to read — {str(e)}")
        cat_failed_regions.append(region)

if not cat_dfs:
    # Category failure is non-fatal — Gold job handles missing category names gracefully
    logger.warn("All regions failed for category reference data. Skipping reference write.")
    logger.warn("Gold job will use category_id numbers instead of names.")
else:
    if cat_failed_regions:
        logger.warn(f"Category regions skipped: {cat_failed_regions}")

    # Union all regions
    raw_cat_df = cat_dfs[0]
    for cat_region_df in cat_dfs[1:]:
        raw_cat_df = raw_cat_df.unionByName(cat_region_df, allowMissingColumns=True)

    logger.info(f"Total raw category records: {raw_cat_df.count()}")

    # ── Flatten category structure ────────────────────────────────────────────
    # YouTube category JSON structure:
    # item.id           → category ID (e.g. "10")
    # item.snippet.title → category name (e.g. "Music")
    # item.snippet.assignable → whether videos can be assigned this category
    # ─────────────────────────────────────────────────────────────────────────
    cat_df_flat = raw_cat_df.select(
        F.col("item.id").cast(LongType()).alias("category_id"),
        F.col("item.snippet.title").cast(StringType()).alias("category_name"),
        F.col("item.snippet.assignable").cast("boolean").alias("assignable"),
        F.col("item.etag").cast(StringType()).alias("etag"),
        F.col("region"),
        F.to_timestamp(F.col("ingestion_timestamp")).alias("ingestion_timestamp"),
    )

    # Drop rows where category_id or category_name is null
    cat_df_flat = cat_df_flat.filter(
        F.col("category_id").isNotNull() & F.col("category_name").isNotNull()
    )

    # Deduplicate — keep one row per category_id + region
    # Categories don't change, but multiple pipeline runs write multiple files
    cat_dedup_window = Window \
        .partitionBy("category_id", "region") \
        .orderBy(F.col("ingestion_timestamp").desc())

    cat_df_flat = cat_df_flat \
        .withColumn("_row_num", F.row_number().over(cat_dedup_window)) \
        .filter(F.col("_row_num") == 1) \
        .drop("_row_num")

    cat_df_flat = cat_df_flat.withColumn("_processed_at", F.current_timestamp())

    cat_clean_count = cat_df_flat.count()
    logger.info(f"Clean category records: {cat_clean_count}")

    # ── Write category reference to Silver ────────────────────────────────────
    # Partitioned by region only — not by date since categories are static.
    # overwrite_partitions = idempotent per region.
    # ─────────────────────────────────────────────────────────────────────────
    logger.info(f"Writing {cat_clean_count} category records to Silver: {SILVER_CATEGORY_PATH}")

    cat_df_flat.write \
        .format("parquet") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .partitionBy("region") \
        .save(SILVER_CATEGORY_PATH)

    logger.info(f"Silver category write complete — {cat_clean_count} records")

    logger.info(f"Category reference write complete — {cat_clean_count} records written")
    logger.info(f"Silver tables written: clean_statistics, clean_reference_data")

job.commit()