data "aws_ami" "node" {
  most_recent = true

  filter {
    name = "name"
    values = ["dag-node-v1"]
  }

  filter {
    name = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["150340915792"]
}

locals {
  ssh_user = "admin"
}