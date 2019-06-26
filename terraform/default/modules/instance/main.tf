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

// Terraform plugin for creating random ids
resource "random_id" "instance_id" {
  byte_length = 4
}

// A single Google Cloud Engine instance
resource "google_compute_instance" "default" {
  count          = var.instance_count
  name           = "constellation-${var.random_id}-${random_id.instance_id.hex}-${count.index}"
  machine_type   = "n1-highcpu-4"
  zone           = var.zone
  can_ip_forward = true


  tags = ["constellation-vm-${var.random_id}", "constellation-${var.random_id}-${random_id.instance_id.hex}"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-1804-lts"
      size  = 200
    }
    auto_delete = true
  }

  // Make sure needed apps are installed on all new instances for later steps
  metadata_startup_script = <<SCRIPT
    sudo apt update
    sudo apt install -yq build-essential haveged rsync google-cloud-sdk openjdk-8-jre-headless
  SCRIPT

  service_account {
    scopes = ["compute-ro", "storage-rw", "monitoring-write", "logging-write"]
  }

  network_interface {
    network = var.network.name

    access_config {
      // Include this section to give the VM an external ip address
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
    source = "constellation.service"
    destination = "/tmp/constellation.service"

    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }


  provisioner "file" {
    source = "start"
    destination = "/tmp/start"

    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }

  provisioner "file" {
    source = "dag"
    destination = "/tmp/dag"

    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }

  provisioner "remote-exec" {
    inline = [
      "until gsutil -v; do echo 'waiting for gsutil...'; sleep 5; done",
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
    inline = ["until java -version; do echo 'waiting for java...'; sleep 5; done"]
    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }

  provisioner "remote-exec" {
    inline = [
      "sudo systemctl start constellation"
    ]
    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }

  provisioner "remote-exec" {
    inline = ["curl -sSO https://dl.google.com/cloudagents/install-monitoring-agent.sh",
    "sudo bash install-monitoring-agent.sh"]
    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }

}
