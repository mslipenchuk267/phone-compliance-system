resource "aws_ecr_repository" "pipeline" {
  name                 = var.ecr_repository_name
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  tags                 = var.tags
}
