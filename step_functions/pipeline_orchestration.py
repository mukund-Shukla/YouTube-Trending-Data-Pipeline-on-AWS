{
  "Comment": "YouTube Trending Data Pipeline — API-only, Medallion Architecture (Bronze → Silver → Gold). Orchestrates ingestion, ETL, Silver catalog refresh, data quality gate, Gold aggregation, and Gold catalog refresh. All layers use Snappy Parquet on S3, registered in Glue Data Catalog, queryable via Athena.",
  "StartAt": "IngestFromYouTubeAPI",
  "States": {

    "IngestFromYouTubeAPI": {
      "Type": "Task",
      "Comment": "Fetches top 50 trending videos + category mappings for 6 regions (US, IN, GB, JP, KR, CA) from YouTube Data API v3. Writes raw JSON to Bronze S3 with Hive-style partitioning: region={r}/date={d}/hour={h}/.",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:ap-south-1:092205142896:function:yt-data-pipeline-youtube-ingestion-prod",
        "Payload": {
          "triggered_by": "step_functions",
          "execution_id.$": "$$.Execution.Id"
        }
      },
      "ResultPath": "$.ingestion_result",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException",
            "Lambda.SdkClientException"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyIngestionFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "RunBronzeToSilver"
    },

    "RunBronzeToSilver": {
      "Type": "Task",
      "Comment": "Single Glue job handles both Silver outputs in one run: (1) clean_statistics — flattened, cleansed, deduplicated trending video facts partitioned by region+date. (2) clean_reference_data — category ID to name lookup partitioned by region. Consolidating both into one job ensures Silver tables are always in sync.",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "yt-data-pipeline-bronze-to-silver-prod",
        "Arguments": {
          "--bronze_database": "yt_pipeline_bronze_prod",
          "--bronze_table": "raw_statistics",
          "--bronze_bucket": "yt-data-pipeline-bronze-ap-south-1-prod",
          "--silver_bucket": "yt-data-pipeline-silver-ap-south-1-prod",
          "--silver_database": "yt_pipeline_silver_prod",
          "--silver_table": "clean_statistics"
        }
      },
      "ResultPath": "$.bronze_silver_result",
      "Retry": [
        {
          "ErrorEquals": ["States.ALL"],
          "IntervalSeconds": 60,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyTransformFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "StartSilverCrawler"
    },

    "StartSilverCrawler": {
      "Type": "Task",
      "Comment": "Starts the Silver Glue crawler after every Bronze→Silver write. Registers new trending_date partitions in the Silver Glue catalog so the DQ Lambda Athena queries always run against fresh, complete data. Without this, new partitions exist in S3 but Athena silently skips them — DQ checks run on stale data and today's records are never validated.",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {
        "Name": "yt-silver-crawler-prod"
      },
      "ResultPath": "$.silver_crawler_start_result",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyTransformFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "WaitForSilverCrawler"
    },

    "WaitForSilverCrawler": {
      "Type": "Task",
      "Comment": "Polls Silver crawler status every 30 seconds until State = READY. Step Functions has no native crawler.sync integration so we poll getCrawler manually. Typical runtime 60-90 seconds — expect 2-3 polling loops.",
      "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
      "Parameters": {
        "Name": "yt-silver-crawler-prod"
      },
      "ResultPath": "$.silver_crawler_status",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyTransformFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "IsSilverCrawlerDone"
    },

    "IsSilverCrawlerDone": {
      "Type": "Choice",
      "Comment": "READY = Silver catalog updated, safe to run DQ checks. Anything else = wait 30 more seconds and poll again.",
      "Choices": [
        {
          "Variable": "$.silver_crawler_status.Crawler.State",
          "StringEquals": "READY",
          "Next": "RunDataQualityChecks"
        }
      ],
      "Default": "SilverCrawlerWait"
    },

    "SilverCrawlerWait": {
      "Type": "Wait",
      "Comment": "30 second pause before polling Silver crawler status again.",
      "Seconds": 30,
      "Next": "WaitForSilverCrawler"
    },

    "RunDataQualityChecks": {
      "Type": "Task",
      "Comment": "Validates Silver layer before Gold aggregation runs. Checks: row count, null %, schema presence, value ranges (views, engagement_rate, like_ratio, views_tier enum, region enum), and data freshness. Freshness is skipped for clean_reference_data since category mappings are static. Silver catalog is guaranteed fresh at this point — Silver crawler completed before this step runs. If any check fails, pipeline halts here and SNS alert is sent — Gold never receives stale or corrupt data.",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:ap-south-1:092205142896:function:yt-data-pipeline-data-quality-prod",
        "Payload": {
          "layer": "silver",
          "database": "yt_pipeline_silver_prod",
          "tables": [
            "clean_statistics",
            "clean_reference_data"
          ]
        }
      },
      "ResultPath": "$.dq_result",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 15,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyDQFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "EvaluateDataQuality"
    },

    "EvaluateDataQuality": {
      "Type": "Choice",
      "Comment": "Routes pipeline based on DQ result. quality_passed=true proceeds to Gold aggregation. quality_passed=false sends SNS alert and halts — Gold job is never triggered on bad data.",
      "Choices": [
        {
          "Variable": "$.dq_result.Payload.quality_passed",
          "BooleanEquals": true,
          "Next": "RunSilverToGold"
        }
      ],
      "Default": "NotifyDQFailure"
    },

    "RunSilverToGold": {
      "Type": "Task",
      "Comment": "Produces four Gold tables from joined Silver data: (1) trending_analytics — daily regional summaries with views tier distribution and HD %. (2) channel_analytics — channel performance with regional rankings. (3) category_analytics — category breakdowns with view share % per region per day. (4) video_velocity — view/like growth rate using LAG() across consecutive trending appearances, with momentum labels (surging/growing/stable/declining/fading). Incremental load: appends new dates, overwrites today for fresh view counts, never touches historical partitions. Reads Silver directly from S3 — no catalog dependency.",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "yt-data-pipeline-silver-to-gold-prod",
        "Arguments": {
          "--silver_database": "yt_pipeline_silver_prod",
          "--silver_bucket": "yt-data-pipeline-silver-ap-south-1-prod",
          "--gold_bucket": "yt-data-pipeline-gold-ap-south-1-prod",
          "--gold_database": "yt_pipeline_gold_prod"
        }
      },
      "ResultPath": "$.gold_result",
      "Retry": [
        {
          "ErrorEquals": ["States.ALL"],
          "IntervalSeconds": 60,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyGoldFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "StartGoldCrawler"
    },

    "StartGoldCrawler": {
      "Type": "Task",
      "Comment": "Starts the Gold Glue crawler after every successful Gold write. Registers new trending_date partitions in the Gold Glue catalog so Athena can query them immediately — no manual MSCK REPAIR TABLE needed.",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": {
        "Name": "yt-gold-crawler-prod"
      },
      "ResultPath": "$.gold_crawler_start_result",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyGoldFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "WaitForGoldCrawler"
    },

    "WaitForGoldCrawler": {
      "Type": "Task",
      "Comment": "Polls Gold crawler status every 30 seconds until State = READY. Typical Gold crawler runtime for 4 small tables is 60-90 seconds — expect 2-3 polling loops.",
      "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
      "Parameters": {
        "Name": "yt-gold-crawler-prod"
      },
      "ResultPath": "$.gold_crawler_status",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "Next": "NotifyGoldFailure",
          "ResultPath": "$.error"
        }
      ],
      "Next": "IsGoldCrawlerDone"
    },

    "IsGoldCrawlerDone": {
      "Type": "Choice",
      "Comment": "READY = Gold catalog updated, Athena can query new partitions. Anything else = wait 30 more seconds and poll again.",
      "Choices": [
        {
          "Variable": "$.gold_crawler_status.Crawler.State",
          "StringEquals": "READY",
          "Next": "NotifySuccess"
        }
      ],
      "Default": "GoldCrawlerWait"
    },

    "GoldCrawlerWait": {
      "Type": "Wait",
      "Comment": "30 second pause before polling Gold crawler status again.",
      "Seconds": 30,
      "Next": "WaitForGoldCrawler"
    },

    "NotifySuccess": {
      "Type": "Task",
      "Comment": "Publishes success notification via SNS on full pipeline completion including both catalog refreshes.",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:ap-south-1:092205142896:yt-data-pipeline-alerts-prod",
        "Subject": "[YT Pipeline] ✅ Pipeline completed successfully",
        "Message.$": "States.Format('Pipeline execution {} completed successfully.\n\nAll layers updated:\n  Bronze  → raw JSON ingested for 6 regions\n  Silver  → clean_statistics + clean_reference_data written\n  Catalog → Silver partitions registered via Glue crawler\n  DQ Gate → 14 checks passed\n  Gold    → trending_analytics, channel_analytics, category_analytics, video_velocity\n  Catalog → Gold partitions registered via Glue crawler\n\nQuery your data in Athena: database = yt_pipeline_gold_prod', $$.Execution.Id)"
      },
      "End": true
    },

    "NotifyIngestionFailure": {
      "Type": "Task",
      "Comment": "Alert on ingestion Lambda failure — Bronze layer not updated.",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:ap-south-1:092205142896:yt-data-pipeline-alerts-prod",
        "Subject": "[YT Pipeline] ❌ FAILED — Ingestion step failed",
        "Message.$": "States.Format('Pipeline execution {} failed at ingestion step.\n\nError: {}\n\nBronze layer was NOT updated. Check Lambda logs in CloudWatch:\nLog group: /aws/lambda/yt-data-pipeline-youtube-ingestion-prod', $$.Execution.Id, States.JsonToString($.error))"
      },
      "End": true
    },

    "NotifyTransformFailure": {
      "Type": "Task",
      "Comment": "Alert on Bronze→Silver Glue job or Silver crawler failure — Silver layer may not be fully updated or catalog may be stale.",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:ap-south-1:092205142896:yt-data-pipeline-alerts-prod",
        "Subject": "[YT Pipeline] ❌ FAILED — Bronze to Silver transform or Silver catalog refresh failed",
        "Message.$": "States.Format('Pipeline execution {} failed at Bronze→Silver Glue job or Silver crawler step.\n\nError: {}\n\nSilver layer may NOT be updated or catalog may be stale. Check:\n  Glue job logs — yt-data-pipeline-bronze-to-silver-prod\n  Glue crawler  — yt-silver-crawler-prod', $$.Execution.Id, States.JsonToString($.error))"
      },
      "End": true
    },

    "NotifyDQFailure": {
      "Type": "Task",
      "Comment": "Alert on DQ gate failure — Gold job intentionally blocked. Full check details included in message.",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:ap-south-1:092205142896:yt-data-pipeline-alerts-prod",
        "Subject": "[YT Pipeline] ⚠️ WARNING — Data quality checks failed, Gold blocked",
        "Message.$": "States.Format('Pipeline execution {} halted at data quality gate.\n\nGold aggregation was NOT run — Silver data did not pass quality checks.\n\nDQ Result: {}\n\nInvestigate Silver tables in Athena:\n  SELECT * FROM yt_pipeline_silver_prod.clean_statistics LIMIT 100;\n  SELECT * FROM yt_pipeline_silver_prod.clean_reference_data LIMIT 100;', $$.Execution.Id, States.JsonToString($.dq_result))"
      },
      "End": true
    },

    "NotifyGoldFailure": {
      "Type": "Task",
      "Comment": "Alert on Silver→Gold Glue job or Gold crawler failure — Gold tables may not be updated but Silver is intact.",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:ap-south-1:092205142896:yt-data-pipeline-alerts-prod",
        "Subject": "[YT Pipeline] ❌ FAILED — Silver to Gold aggregation or Gold catalog refresh failed",
        "Message.$": "States.Format('Pipeline execution {} failed at Silver→Gold Glue job or Gold crawler step.\n\nError: {}\n\nSilver data is intact and valid (passed DQ checks).\nGold tables may NOT be updated. Check:\n  Glue job logs — yt-data-pipeline-silver-to-gold-prod\n  Glue crawler  — yt-gold-crawler-prod', $$.Execution.Id, States.JsonToString($.error))"
      },
      "End": true
    }

  }
}