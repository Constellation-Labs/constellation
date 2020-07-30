variable "aws_region" {
  type = string
  default = "us-west-1"
}

variable "instance_type" {
  type = string
  default = "t3.xlarge"
}

variable "grafana_disk_size" {
  type = number
  default = 50
}

variable "cluster_info_url" {
  type = string
  default = ""
  description = "Url to /cluster/info"
}