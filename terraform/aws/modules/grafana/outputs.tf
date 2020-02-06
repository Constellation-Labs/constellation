output "grafana_ip" {
  value = aws_instance.node[0].public_ip
}

output "ssh_user" {
  value = local.ssh_user
}