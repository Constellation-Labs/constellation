resource "random_id" "instance_id" {
  byte_length = 4
}

locals {
  cluster_id = random_id.instance_id.hex
}


module "grafana" {
  source = "./modules/grafana"
  cluster_id = local.cluster_id
  env = var.env
  workspace = terraform.workspace
  instance_type = var.instance_type
  disk_size = var.grafana_disk_size
}

module "nodes" {
  source = "./modules/node"
  instance_count = var.instance_count
  cluster_id = local.cluster_id
  env = var.env
  workspace = terraform.workspace
  app_port = var.node_app_port
  instance_type = var.instance_type
  grafana_ip = module.grafana.grafana_ip
  disk_size = var.node_disk_size
}

module "provisioner" {
  source = "./modules/provisioner"
  ssh_user = module.grafana.ssh_user
  grafana_ip = module.grafana.grafana_ip
  ips_for_grafana = module.nodes.instance_ips_grafana
}
