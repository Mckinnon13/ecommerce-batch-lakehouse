terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Create the Raw Zone Bucket
resource "aws_s3_bucket" "raw_zone" {
  bucket = var.raw_bucket_name
}

# Create the Staging Zone Bucket
resource "aws_s3_bucket" "staging_zone" {
  bucket = var.staging_bucket_name
}

# Create the Curated Zone Bucket
resource "aws_s3_bucket" "curated_zone" {
  bucket = var.curated_bucket_name
}

# Create the IAM Role for Glue
resource "aws_iam_role" "glue_role" {
  name = "OlistGlueServiceRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# Attach S3 Access to the Role
resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Attach Basic Glue Service Access (for logging)
resource "aws_iam_role_policy_attachment" "glue_service_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}