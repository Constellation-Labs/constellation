output "cluster_id" {
  value = local.cluster_id
}

output "workspace" {
  value = terraform.workspace
}

output "instance_ips" {
  value = module.nodes.instance_ips
}

output "grafana_ip" {
  value = module.grafana.grafana_ip
}
