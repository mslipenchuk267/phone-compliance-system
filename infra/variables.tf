variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

# ── S3 ───────────────────────────────────────────────────────────────────────

variable "s3_data_bucket_name" {
  description = "S3 bucket name for raw input data (gzipped CSVs)"
  type        = string
}

variable "s3_delta_bucket_name" {
  description = "S3 bucket name for Delta Lake tables (bronze/silver/gold)"
  type        = string
}

# ── ECR ──────────────────────────────────────────────────────────────────────

variable "ecr_repository_name" {
  description = "ECR repository name for the pipeline Docker image"
  type        = string
  default     = "phone-compliance-pipeline"
}

# ── Databricks ───────────────────────────────────────────────────────────────

variable "databricks_host" {
  description = "Databricks workspace URL (e.g. https://dbc-xxxxx.cloud.databricks.com)"
  type        = string
}

variable "databricks_token" {
  description = "Databricks personal access token or service principal token"
  type        = string
  sensitive   = true
}

variable "spark_version" {
  description = "Databricks runtime version"
  type        = string
  default     = "14.3.x-scala2.12"
}

variable "node_type_id" {
  description = "Databricks cluster node type"
  type        = string
  default     = "i3.xlarge"
}

variable "num_workers" {
  description = "Number of worker nodes for the job cluster"
  type        = number
  default     = 2
}

variable "databricks_cross_account_role_name" {
  description = "Name of the Databricks cross-account IAM role (created during workspace setup, e.g. databricks-compute-role-XXXXX)"
  type        = string
}

# ── S3 credentials for Databricks clusters ─────────────────────────────────
# Required when instance profiles are unavailable (e.g. free-trial workspaces).

variable "aws_access_key_id" {
  description = "AWS access key ID for Databricks clusters to access S3"
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key for Databricks clusters to access S3"
  type        = string
  sensitive   = true
}

# ── General ──────────────────────────────────────────────────────────────────

variable "tags" {
  description = "Tags applied to all AWS resources"
  type        = map(string)
  default     = { Project = "phone-compliance" }
}
