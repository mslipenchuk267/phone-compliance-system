# ── Grant Databricks cross-account role direct S3 access ─────────────────────

resource "aws_iam_role_policy" "databricks_s3_access" {
  name = "phone-compliance-s3-access"
  role = var.databricks_cross_account_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
          aws_s3_bucket.delta.arn,
          "${aws_s3_bucket.delta.arn}/*",
        ]
      }
    ]
  })
}
