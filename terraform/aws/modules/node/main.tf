resource "aws_instance" "node" {
  count = var.instance_count

  ami = data.aws_ami.node.id
  instance_type = var.instance_type
  associate_public_ip_address = true

  root_block_device {
    volume_type = "gp2"
    volume_size = var.disk_size
  }

  user_data = file("ssh_keys.sh")

  tags = {
    Name = "node-${var.cluster_id}-${count.index}"
    Env = var.env
    Cluster = var.cluster_id
    Workspace = var.workspace
  }

  provisioner "file" {
    source = "${path.module}/templates/setup.sh"
    destination = "/tmp/setup.sh"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "240s"
    }
  }

  provisioner "file" {
    source = "${path.module}/templates/logback.xml"
    destination = "/tmp/logback.xml"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "240s"
    }
  }

  provisioner "file" {
    content = templatefile("${path.module}/templates/filebeat.yml.tpl", { es_ip = var.grafana_ip })
    destination = "/tmp/filebeat.yml"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "240s"
    }
  }

  provisioner "file" {
    source = "${path.module}/templates/start_genesis"
    destination = "/tmp/start_genesis"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "240s"
    }
  }

    provisioner "file" {
      source = "${path.module}/templates/start_rollback"
      destination = "/tmp/start_rollback"

      connection {
        host = self.public_ip
        type = "ssh"
        user = local.ssh_user
        timeout = "240s"
      }
    }

  provisioner "file" {
    source = "${path.module}/templates/start_node"
    destination = "/tmp/start_node"

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "240s"
    }
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /tmp/setup.sh",
      "/tmp/setup.sh ${terraform.workspace} ${count.index}"
    ]

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "240s"
    }
  }

  provisioner "remote-exec" {
    inline = [
      "sudo cp /tmp/filebeat.yml /etc/filebeat/filebeat.yml",
      "sudo service filebeat restart",
      "sudo systemctl start constellation"
    ]

    connection {
      host = self.public_ip
      type = "ssh"
      user = local.ssh_user
      timeout = "240s"
    }
  }
}
