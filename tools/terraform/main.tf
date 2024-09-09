terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  alias  = "control"
  region = "ap-south-1"
}

provider "aws" {
  alias  = "ap"
  region = "ap-east-1"
}

provider "aws" {
  alias  = "us"
  region = "us-west-1"
}

provider "aws" {
  alias  = "eu"
  region = "eu-central-1"
}

provider "aws" {
  alias  = "sa"
  region = "sa-east-1"
}

provider "aws" {
  alias  = "af"
  region = "af-south-1"
}

variable "state" {
  type    = string
  default = "running"
}

variable "mode" {
  type    = string
  validation {
    condition     = contains(["latency", "tput"], var.mode)
    error_message = "Unexpected mode."
  }
}

module "ap" {
  source = "./region"
  count  = 1
  providers = {
    aws = aws.ap
  }

  mode  = var.mode
  state = var.state
}

module "us" {
  source = "./region"
  count  = var.mode == "tput" ? 0 : 1
  providers = {
    aws = aws.us
  }

  mode  = var.mode
  state = var.state
}

module "eu" {
  source = "./region"
  count  = var.mode == "tput" ? 0 : 1
  providers = {
    aws = aws.eu
  }

  mode  = var.mode
  state = var.state
}

module "sa" {
  source = "./region"
  count  = var.mode == "tput" ? 0 : 1
  providers = {
    aws = aws.sa
  }

  mode  = var.mode
  state = var.state
}

module "af" {
  source = "./region"
  count  = var.mode == "tput" ? 0 : 1
  providers = {
    aws = aws.af
  }

  mode  = var.mode
  state = var.state
}

output "instances" {
  value = {
    regions = var.mode == "tput" ? {
      ap = module.ap[0].instances
    } : {
      ap = module.ap[0].instances
      us = module.us[0].instances
      eu = module.eu[0].instances
      sa = module.sa[0].instances
      af = module.af[0].instances
    }
  }
}
