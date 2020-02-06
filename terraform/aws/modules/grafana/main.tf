resource "aws_instance" "node" {
  count = 1

  ami = data.aws_ami.node.id
  instance_type = var.instance_type
  associate_public_ip_address = true

  ebs_block_device {
    device_name = "/dev/xvdb"
    volume_type = "gp2"
    volume_size = 100
  }

  user_data = file("ssh_keys.sh")

  tags = {
    Name = "grafana-${var.cluster_id}"
    Env = var.env
    Cluster = var.cluster_id
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
}
