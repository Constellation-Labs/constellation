terraform {
  backend "gcs" {
    bucket = "constellation-tf"
    prefix = "terraform/state-default"
  }
}

variable "ssh_user" {
  type = string
}

variable "project_name" {
  type    = string
  default = "esoteric-helix-197319"
}

variable "region" {
  type    = string
  default = "us-west2"
}

variable "zone" {
  type    = string
  default = "us-west2-b"
}

// Configure the Google Cloud provider
provider "google" {
  project = var.project_name
  region  = var.region
  version = "~> 2.0"
}

// Terraform plugin for creating random ids
resource "random_id" "instance_id" {
  byte_length = 4
}

output "cluster_tag" {
  value = "constellation-${random_id.instance_id.hex}"
}

output "instance_ips" {
  value = module.nodes.instance_ips
}

output "grafana_ip" {
  value = module.grafana.grafana_ip
}

module "network" {
  source    = "./modules/network"
  random_id = random_id.instance_id.hex
}

module "nodes" {
  source         = "./modules/instance"
  zone           = var.zone
  instance_count = 3
  ssh_user       = var.ssh_user
  network_name   = module.network.network_name
  random_id      = random_id.instance_id.hex
}

module "grafana" {
  source          = "./modules/grafana"
  zone            = var.zone
  ssh_user        = var.ssh_user
  network_name    = module.network.network_name
  random_id       = random_id.instance_id.hex
  ips_for_grafana = module.nodes.ips_for_grafana
}

