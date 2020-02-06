variable "aws_region" {
  default = "us-west-1"
}

variable "env" {
  default = "dev"
}

variable "node_app_port" {
  default = "9000"
}

variable "instance_count" {
  type = number
  default = 3
}