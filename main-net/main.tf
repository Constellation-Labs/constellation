module "grafana" {
  source = "./modules/grafana"
  cluster_id = "mainnet"
  env = "mainnet"
  workspace = terraform.workspace
  instance_type = var.instance_type
  disk_size = var.grafana_disk_size
  whitelisting_file_url = var.whitelisting_file_url
  cluster_info_url = var.cluster_info_url
  discovery_mode = var.discovery_mode
}
