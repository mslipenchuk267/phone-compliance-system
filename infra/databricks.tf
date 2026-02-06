# ── Multi-task job: Bronze → Silver → Gold ───────────────────────────────────

resource "databricks_job" "phone_compliance_pipeline" {
  name = "phone-compliance-pipeline"

  job_cluster {
    job_cluster_key = "pipeline_cluster"
    new_cluster {
      spark_version = var.spark_version
      node_type_id  = var.node_type_id
      num_workers   = 0

      aws_attributes {
        availability = "SPOT_WITH_FALLBACK"
      }

      spark_conf = {
        "spark.sql.extensions"                                            = "io.delta.sql.DeltaSparkSessionExtension"
        "spark.sql.catalog.spark_catalog"                                 = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed" = "true"
        "spark.hadoop.fs.s3a.access.key"                                 = var.aws_access_key_id
        "spark.hadoop.fs.s3a.secret.key"                                 = var.aws_secret_access_key
        "spark.databricks.cluster.profile"                               = "singleNode"
        "spark.master"                                                   = "local[*, 4]"
      }

      custom_tags = {
        "ResourceClass" = "SingleNode"
      }
    }
  }

  # ── Task 1: Bronze ──────────────────────────────────────────────────────────

  task {
    task_key        = "bronze"
    job_cluster_key = "pipeline_cluster"

    spark_python_task {
      python_file = "dbfs:/phone-compliance/pipeline/run_bronze_cloud.py"
      parameters = [
        "s3://${var.s3_data_bucket_name}",
        "s3://${var.s3_delta_bucket_name}/bronze",
      ]
    }
  }

  # ── Task 2: Silver ──────────────────────────────────────────────────────────

  task {
    task_key        = "silver"
    job_cluster_key = "pipeline_cluster"

    depends_on {
      task_key = "bronze"
    }

    spark_python_task {
      python_file = "dbfs:/phone-compliance/pipeline/run_silver_cloud.py"
      parameters = [
        "s3://${var.s3_delta_bucket_name}/bronze",
        "s3://${var.s3_delta_bucket_name}/silver",
      ]
    }
  }

  # ── Task 3: Gold ────────────────────────────────────────────────────────────

  task {
    task_key        = "gold"
    job_cluster_key = "pipeline_cluster"

    depends_on {
      task_key = "silver"
    }

    spark_python_task {
      python_file = "dbfs:/phone-compliance/pipeline/run_gold_cloud.py"
      parameters = [
        "s3://${var.s3_delta_bucket_name}/silver",
        "s3://${var.s3_delta_bucket_name}/gold",
      ]
    }
  }
}
