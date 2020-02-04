terraform {
  backend "gcs" {
    bucket = "constellation-tf"
    prefix = "terraform/state-buildbox"
  }
}

variable "ssh_user" {
  type = "string"
}

variable "project_name" {
  type    = "string"
  default = "esoteric-helix-197319"
}

variable "region" {
  type    = "string"
  default = "us-west2"
}

// Configure the Google Cloud provider
provider "google" {
  project = var.project_name
  region  = var.region
}

// Terraform plugin for creating random ids
resource "random_id" "instance_id" {
  byte_length = 2
}

output "instance_ips" {
  value = google_compute_instance.default.*.network_interface.0.access_config.0.nat_ip
}

// A single Google Cloud Engine instance
resource "google_compute_instance" "default" {
  count                     = 1
  name                      = "buildbox-${random_id.instance_id.hex}-${count.index}"
  machine_type              = "n1-highcpu-4"
  zone                      = "us-west2-b"
  can_ip_forward            = true
  allow_stopping_for_update = true


  tags = ["buildbox-vm-${random_id.instance_id.hex}"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-1604-lts"
      size  = 100
    }
    auto_delete = true
  }

  // Make sure needed apps are installed on all new instances for later steps
  metadata_startup_script = "echo 'deb https://dl.bintray.com/sbt/debian /' | sudo tee -a /etc/apt/sources.list.d/sbt.list; sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823; sudo apt update; sudo apt install -yq build-essential haveged rsync google-cloud-sdk openjdk-8-jdk-headless sbt"

  service_account {
    scopes = ["compute-ro", "storage-full", "monitoring-write", "logging-write"]
  }

  network_interface {
    network = "default"

    access_config {
      // Include this section to give the VM an external ip address
    }
  }

  scheduling {
    preemptible       = "true" // "false"
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

  provisioner "remote-exec" {
    inline = [
      "git clone https://github.com/Constellation-Labs/constellation.git",
    ]
    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }

  provisioner "file" {
    source      = "upload_jar.sh"
    destination = "~/upload_jar.sh"

    connection {
      host    = self.network_interface.0.access_config.0.nat_ip
      type    = "ssh"
      user    = var.ssh_user
      timeout = "90s"
    }
  }

  provisioner "file" {
    source      = "deploy.sh"
    destination = "~/deploy.sh"

    connection {
      host    = self.network_interface.0.access_config.0.nat_ip
      type    = "ssh"
      user    = var.ssh_user
      timeout = "90s"
    }
  }

  provisioner "remote-exec" {
    inline = [
      "chmod u+x ~/upload_jar.sh",
      "chmod u+x ~/deploy.sh",
    ]
    connection {
      host = self.network_interface.0.access_config.0.nat_ip
      type = "ssh"
      user = var.ssh_user
    }
  }


}
