output "grafana_ip" {
  value = aws_instance.node.public_ip
}

output "ssh_user" {
  value = local.ssh_user
}