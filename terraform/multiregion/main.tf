terraform {
  backend "gcs" {
    bucket  = "constellation-tf"
    prefix  = "terraform/state-multiregion"
  }
}

variable "project_name" {
  type    = "string",
  default = "esoteric-helix-197319"
}

variable "region" {
  type    = "string",
  default = "us-west2"
}

variable "ssh_user" {
  type = "string"
}

// Configure the Google Cloud provider
provider "google" {
 project     = "${var.project_name}"
 region      = "${var.region}"
}

// Terraform plugin for creating random ids
resource "random_id" "instance_id" {
 byte_length = 4
}

module "network" {
  source = "./network"
  random_id = "${random_id.instance_id.hex}"
}

module "instance_a" {
  source = "./instance"
  zone = "us-west2-a"
  instance_count = 1
  ssh_user = "${var.ssh_user}"
  network_name = "${module.network.network_name}"
  random_id = "${random_id.instance_id.hex}"
}

module "instance_b" {
  source = "./instance"
  zone = "us-east1-b"
  instance_count = 1
  ssh_user = "${var.ssh_user}"
  network_name = "${module.network.network_name}"
  random_id = "${random_id.instance_id.hex}"
}

module "instance_c" {
  source = "./instance"
  zone = "us-central1-f"
  instance_count = 1
  ssh_user = "${var.ssh_user}"
  network_name = "${module.network.network_name}"
  random_id = "${random_id.instance_id.hex}"
}

module "instance_d" {
  source = "./instance"
  zone = "europe-west3-b"
  instance_count = 1
  ssh_user = "${var.ssh_user}"
  network_name = "${module.network.network_name}"
  random_id = "${random_id.instance_id.hex}"
}

module "instance_e" {
  source = "./instance"
  zone = "asia-east1-c"
  instance_count = 1
  ssh_user = "${var.ssh_user}"
  network_name = "${module.network.network_name}"
  random_id = "${random_id.instance_id.hex}"
}

module "instance_f" {
  source = "./instance"
  zone = "australia-southeast1-b"
  instance_count = 1
  ssh_user = "${var.ssh_user}"
  network_name = "${module.network.network_name}"
  random_id = "${random_id.instance_id.hex}"
}

module "instance_g" {
  source = "./instance"
  zone = "southamerica-east1-b"
  instance_count = 1
  ssh_user = "${var.ssh_user}"
  network_name = "${module.network.network_name}"
  random_id = "${random_id.instance_id.hex}"
}

output "cluster_tag" {
  value = "constellation-${random_id.instance_id.hex}"
}
