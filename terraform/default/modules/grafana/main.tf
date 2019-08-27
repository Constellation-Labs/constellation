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

// Grafana GCP Instance
resource "google_compute_instance" "grafana" {
  count                     = 1
  name                      = "grafana-${var.random_id}-${count.index}"
  machine_type              = "n1-standard-1"
  zone                      = var.zone
  can_ip_forward            = true
  allow_stopping_for_update = true


  tags = ["constellation-vm-${var.random_id}", "grafana-vm-${var.random_id}"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-1804-lts"
      size  = 100
    }
    auto_delete = true
  }

  // Make sure needed apps are installed on all new instances for later steps
  metadata_startup_script = <<SCRIPT
 sudo apt update
 sudo apt install apt-transport-https ca-certificates curl gnupg-agent software-properties-common
 curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
 sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
 echo 'deb https://dl.bintray.com/sbt/debian /' | sudo tee -a /etc/apt/sources.list.d/sbt.list
 sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
 sudo apt update
 sudo apt install -yq build-essential haveged rsync google-cloud-sdk openjdk-8-jdk-headless sbt docker-ce
 sudo curl -L "https://github.com/docker/compose/releases/download/1.23.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
 sudo chmod +x /usr/local/bin/docker-compose
 sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
 sudo sysctl -w vm.max_map_count=262144
 SCRIPT

  service_account {
    scopes = ["compute-ro", "storage-full", "monitoring-write", "logging-write"]
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

  provisioner "remote-exec" {
    inline = [
      "until gsutil -v; do echo 'waiting for gsutil...'; sleep 5; done",
      "until java -version; do echo 'waiting for java...'; sleep 5; done"
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

}
