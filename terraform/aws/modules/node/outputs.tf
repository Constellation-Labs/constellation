locals {
  instance_ips = aws_instance.node.*.public_ip
}

output "instance_ips" {
  value = local.instance_ips
}

output "instance_ips_grafana" {
  // TODO: hardcoded application port
  value = "[${join(",", formatlist("'%s:%s'", local.instance_ips, var.app_port))}]"
}