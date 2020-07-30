resource "aws_instance" "node" {
  ami = data.aws_ami.grafana.id
  instance_type = var.instance_type
  associate_public_ip_address = true

  root_block_device {
    volume_type = "gp2"
    volume_size = var.disk_size
  }

  user_data = file("user_data.sh")

  tags = {
    Name = "grafana-mainnet"
    Env = var.env
    Cluster = "mainnet"
    Workspace = var.workspace
  }

  provisioner "file" {
    source = "${path.module}/templates/docker-compose.yml"
    destination = "~/docker-compose.yml"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "30s"
    }
  }

  provisioner "file" {
    source = "${path.module}/templates/grafana-dashboard"
    destination = "~/grafana-dashboard"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "30s"
    }
  }

  provisioner "file" {
    content = templatefile("${path.module}/templates/cluster_info_to_targets.sh.tpl", {
      url = var.cluster_info_url
    })
    destination = "~/grafana-dashboard/prometheus/cluster_info_to_targets.sh"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "30s"
    }
  }

  provisioner "file" {
    source = "${path.module}/templates/setup.sh"
    destination = "~/grafana-dashboard/prometheus/setup.sh"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "30s"
    }
  }

  provisioner "file" {
    source = "${path.module}/templates/setup_target_discovery_cron.sh"
    destination = "~/grafana-dashboard/prometheus/setup_target_discovery_cron.sh"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "30s"
    }
  }

  provisioner "remote-exec" {
    inline = [
      "sudo chmod +x ~/grafana-dashboard/prometheus/cluster_info_to_targets.sh",
      "sudo chmod +x ~/grafana-dashboard/prometheus/setup_target_discovery_cron.sh",
      "sudo chmod +x ~/grafana-dashboard/prometheus/whitelisting_to_targets.sh",
      "sudo chmod +x ~/grafana-dashboard/prometheus/setup.sh",
      "sudo ~/grafana-dashboard/prometheus/setup.sh",
    ]

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "30s"
    }
  }

  provisioner "remote-exec" {
    inline = [
      "sudo docker-compose up -d",
    ]

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "30s"
    }
  }
}