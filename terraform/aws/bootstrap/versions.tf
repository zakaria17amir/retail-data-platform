terraform {
  required_version = ">= 1.9"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.65.0"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project = "retail-data-platform"
      env     = var.env
    }
  }
}
