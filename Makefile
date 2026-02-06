# ─── Phone Compliance Pipeline ────────────────────────────────────────────────
#
# Targets:
#   Infrastructure: init, plan, deploy, destroy
#   Data:           push-data, generate-data
#   Docker:         build, push-image
#   Databricks:     push-code, run-pipeline
#   Local dev:      test, test-all, run-local
# ──────────────────────────────────────────────────────────────────────────────

INFRA_DIR  := infra
DATA_DIR   ?= ./data
IMAGE_NAME := phone-compliance-pipeline
IMAGE_TAG  ?= latest

# Derived from Terraform outputs (lazy-evaluated).
AWS_REGION   = $(shell cd $(INFRA_DIR) && terraform output -raw aws_region 2>/dev/null || echo "us-east-1")
DATA_BUCKET  = $(shell cd $(INFRA_DIR) && terraform output -raw data_bucket_name)
DELTA_BUCKET = $(shell cd $(INFRA_DIR) && terraform output -raw delta_bucket_name)
ECR_REPO_URL = $(shell cd $(INFRA_DIR) && terraform output -raw ecr_repository_url)
ECR_REGISTRY = $(shell echo $(ECR_REPO_URL) | cut -d'/' -f1)
JOB_ID       = $(shell cd $(INFRA_DIR) && terraform output -raw databricks_job_id)

.PHONY: help init plan deploy destroy \
        push-data generate-data \
        build push-image \
        push-code run-pipeline \
        test test-all run-local

help:
	@echo "Infrastructure"
	@echo "  init           Terraform init"
	@echo "  plan           Terraform plan"
	@echo "  deploy         Terraform apply (creates S3, ECR, IAM, Databricks job)"
	@echo "  destroy        Terraform destroy"
	@echo ""
	@echo "Data"
	@echo "  push-data      Upload data files to S3  (DATA_DIR=./data)"
	@echo "  generate-data  Generate 1GB test dataset into DATA_DIR"
	@echo ""
	@echo "Docker"
	@echo "  build          Build Docker image"
	@echo "  push-image     Push Docker image to ECR"
	@echo ""
	@echo "Databricks"
	@echo "  push-code      Upload pipeline code to DBFS"
	@echo "  run-pipeline   Trigger the Databricks job"
	@echo ""
	@echo "Local development"
	@echo "  test           Run unit + integration tests"
	@echo "  test-all       Run all tests including E2E"
	@echo "  run-local      Run full pipeline locally on data_sample/"

# ─── Infrastructure ──────────────────────────────────────────────────────────

init:
	cd $(INFRA_DIR) && terraform init

plan:
	cd $(INFRA_DIR) && terraform plan

deploy:
	cd $(INFRA_DIR) && terraform apply

destroy:
	cd $(INFRA_DIR) && terraform destroy

# ─── Data ────────────────────────────────────────────────────────────────────

push-data:
	aws s3 sync $(DATA_DIR)/ENROLLMENT/  s3://$(DATA_BUCKET)/ENROLLMENT/  --exclude "*.DS_Store"
	aws s3 sync $(DATA_DIR)/MAINTENANCE/ s3://$(DATA_BUCKET)/MAINTENANCE/ --exclude "*.DS_Store"

generate-data:
	uv run python generate_bank_data.py --output-dir $(DATA_DIR) --target-size-gb 1.0

# ─── Docker ──────────────────────────────────────────────────────────────────

build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

push-image: build
	aws ecr get-login-password --region $(AWS_REGION) | \
		docker login --username AWS --password-stdin $(ECR_REGISTRY)
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(ECR_REPO_URL):$(IMAGE_TAG)
	docker push $(ECR_REPO_URL):$(IMAGE_TAG)

# ─── Databricks ──────────────────────────────────────────────────────────────

push-code:
	databricks fs cp -r pipeline/ dbfs:/phone-compliance/pipeline/ --overwrite

run-pipeline:
	databricks jobs run-now --job-id $(JOB_ID)

# ─── Local development ──────────────────────────────────────────────────────

test:
	uv run pytest tests/ -v

test-all:
	uv run pytest tests/ -v -m ""

run-local:
	uv run python -m pipeline.run_gold ./data_sample
