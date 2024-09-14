terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "state" {
  type = string
}

variable "mode" {
  type = string
  validation {
    condition     = contains(["latency", "tput"], var.mode)
    error_message = "Unexpected mode."
  }
}

module "network" {
  source = "../group_network"
}

data "aws_region" "_1" {}

module "_1" {
  source = "../group"

  network = module.network
  state   = var.state
  type    = "r6i.4xlarge"
  n       = var.mode == "tput" ? 100 : 20
  # n = 10
}

output "instances" {
  value = module._1.instances
}
