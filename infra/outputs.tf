output "aws_region" {
  value = var.aws_region
}

output "data_bucket_name" {
  value = aws_s3_bucket.data.bucket
}

output "delta_bucket_name" {
  value = aws_s3_bucket.delta.bucket
}

output "ecr_repository_url" {
  value = aws_ecr_repository.pipeline.repository_url
}

output "databricks_job_id" {
  value = databricks_job.phone_compliance_pipeline.id
}

