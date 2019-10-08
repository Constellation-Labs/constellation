variable "zone" {
  type = "string"
}

variable "ssh_user" {
  type = "string"
}

variable "network" {}

variable "random_id" {
  type = "string"
}

output "grafana_ip" {
  value = google_compute_instance.grafana.*.network_interface.0.access_config.0.nat_ip
}

resource "google_compute_instance" "grafana" {
  count                     = 1
  name                      = "grafana-${var.random_id}-${count.index}"
  machine_type              = "n1-highcpu-4"
  zone                      = var.zone
  can_ip_forward            = true
  allow_stopping_for_update = true


  tags = ["constellation-vm-${var.random_id}", "grafana-vm-${var.random_id}"]

  boot_disk {
    initialize_params {
      image = "constellation-labs-ubuntu-grafana-image"
      size  = 100
    }
    auto_delete = true
  }

  metadata_startup_script = <<SCRIPT
    sudo apt update
    sudo sysctl -w vm.max_map_count=262144
  SCRIPT

  service_account {
    scopes = ["compute-ro", "storage-full", "monitoring-write", "logging-write"]
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
    source = "docker-compose.yml"
    destination = "~/docker-compose.yml"

    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
      timeout = "90s"
    }
  }

  provisioner "file" {
    source = "grafana-dashboard"
    destination = "~/grafana-dashboard"

    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
      timeout = "90s"
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
