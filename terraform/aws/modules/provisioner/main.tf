resource "null_resource" "grafana_provisioner" {
  connection {
    host = var.grafana_ip
    type = "ssh"
    user = var.ssh_user
    timeout = "240s"
  }

  provisioner "file" {
    content = templatefile("modules/grafana/templates/prometheus.yml.tpl", { ips_for_grafana = var.ips_for_grafana })
    destination = "~/grafana-dashboard/prometheus/prometheus.yml"
  }

  provisioner "remote-exec" {
    inline = [
      "sudo docker-compose up -d"
    ]
  }
}