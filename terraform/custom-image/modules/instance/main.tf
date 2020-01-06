locals {
  instance_ips = google_compute_instance.default.*.network_interface.0.access_config.0.nat_ip
}

output "instance_ips" {
  value = local.instance_ips
}

output "ips_for_grafana" {
  value = "[${join(",", formatlist("'%s:9000'", local.instance_ips))}]"
}

variable "zone" {
  type = "string"
}

variable "ssh_user" {
  type = "string"
}

variable "instance_count" {
  type = "string"
}

variable "network" {}

variable "random_id" {
  type = "string"
}

variable "grafana_ip" {}

resource "random_id" "instance_id" {
  byte_length = 4
}

resource "google_compute_instance" "default" {
  count          = var.instance_count
  name           = "constellation-${var.random_id}-${random_id.instance_id.hex}-${count.index}"
  machine_type   = "n1-highmem-4"
  zone           = var.zone
  can_ip_forward = true


  tags = ["constellation-vm-${var.random_id}", "constellation-${var.random_id}-${random_id.instance_id.hex}"]

  boot_disk {
    initialize_params {
      image = "constellation-labs-ubuntu-instance-image"
      size  = 200
    }
    auto_delete = true
  }

  metadata_startup_script = <<SCRIPT
    sleep 30
    sudo apt update
    sleep 30
    sudo apt -yq -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confnew install filebeat
  SCRIPT

  service_account {
    scopes = ["compute-ro", "storage-rw", "monitoring-write", "logging-write"]
  }

  network_interface {
    network = var.network.name

    access_config {
    }
  }

  scheduling {
    preemptible = "false"
    automatic_restart = "false"
  }

  provisioner "file" {
    source = "setup.sh"
    destination = "/tmp/setup.sh"

    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
      timeout = "90s"
    }
  }

  provisioner "file" {
    source = "logback.xml"
    destination = "/tmp/logback.xml"

    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
      timeout = "90s"
    }
  }

  provisioner "file" {
    content = templatefile("modules/instance/templates/filebeat.yml.tpl", { es_ip = var.grafana_ip })
    destination = "/tmp/filebeat.yml"

    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
      timeout = "90s"
    }
  }

  provisioner "file" {
    source = "start"
    destination = "/tmp/start"

    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
      timeout = "90s"
    }
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /tmp/setup.sh",
      "/tmp/setup.sh"
    ]
    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }

  provisioner "remote-exec" {
    inline = [
      "sudo cp /tmp/filebeat.yml /etc/filebeat/filebeat.yml",
      "sudo service filebeat restart",
      "sudo systemctl start constellation"
    ]
    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }

  provisioner "remote-exec" {
    inline = [
      "curl -sSO https://dl.google.com/cloudagents/install-monitoring-agent.sh",
      "sudo bash install-monitoring-agent.sh"
    ]
    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }
}
