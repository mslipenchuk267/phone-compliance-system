# ── Raw data bucket (receives gzipped CSVs via `make push-data`) ─────────────

resource "aws_s3_bucket" "data" {
  bucket = var.s3_data_bucket_name
  tags   = var.tags
}

# ── Delta Lake bucket (Bronze / Silver / Gold tables) ────────────────────────

resource "aws_s3_bucket" "delta" {
  bucket = var.s3_delta_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "delta" {
  bucket = aws_s3_bucket.delta.id
  versioning_configuration {
    status = "Enabled"
  }
}
