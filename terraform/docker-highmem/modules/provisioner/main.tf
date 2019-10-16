variable "ssh_user" {
  type = "string"
}

variable "ips_for_grafana" {
  type = "string"
}

variable "grafana_ip" {}

resource "null_resource" "grafana" {
  connection {
    host = var.grafana_ip
    type = "ssh"
    user = var.ssh_user
    timeout = "90s"
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
