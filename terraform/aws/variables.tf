variable "aws_region" {
  type = string
  default = "us-west-1"
}

variable "env" {
  type = string
  default = "dev"
}

variable "node_app_port" {
  type = string
  default = "9000"
}

variable "instance_count" {
  type = number
  default = 3
}

variable "instance_type" {
  type = string
  default = "t3.xlarge"
}

variable "node_disk_size" {
  type = number
  default = 50
}

variable "grafana_disk_size" {
  type = number
  default = 50
}
