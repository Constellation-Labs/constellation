variable "cluster_id" {
  type = string
}

variable "env" {
  type = string
}

variable "workspace" {
  type = string
}

variable "instance_type" {
  type = string
  default = "t2.micro"
}