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

variable "discovery_mode" {
  type = bool
  default = false
  description = "Whether use dynamic node discovery (based on cluster/info/) or static whitelisting file."
}

variable "whitelisting_file_url" {
  type = string
  description = "Url to whitelisting file."
}

variable "cluster_info_url" {
  type = string
  default = ""
  description = "Url to /cluster/info (for discovery_mode)."
}