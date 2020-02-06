variable "cluster_id" {
  type = string
}

variable "instance_count" {
  type = string
}

variable "env" {
  type = string
}

variable "workspace" {
  type = string
}

variable "app_port" {
  type = string
}

variable "instance_type" {
  type = string
  default = "t2.micro"
}

variable "grafana_ip" {
  type = string
}