terraform {
  backend "gcs" {
    bucket  = "constellation-tf"
    prefix  = "terraform/state"
  }
}

variable "ssh_user" {
  type = "string"
}

variable "project_name" {
  type    = "string",
  default = "esoteric-helix-197319"
}

variable "region" {
  type    = "string",
  default = "us-west2"
}

// Configure the Google Cloud provider
provider "google" {
 project     = "${var.project_name}"
 region      = "${var.region}"
}

// Terraform plugin for creating random ids
resource "random_id" "instance_id" {
 byte_length = 4
}

output "instance_ips" {
  value = "${google_compute_instance.default.*.network_interface.0.access_config.0.assigned_nat_ip}"
}

// A single Google Cloud Engine instance
resource "google_compute_instance" "default" {
 count          = 5
 name = "constellation-${random_id.instance_id.hex}-${count.index}"
 machine_type   = "n1-standard-1"
 zone           = "us-west2-b"
 can_ip_forward = true


 tags = ["constellation-vm-${random_id.instance_id.hex}"]

 boot_disk {
   initialize_params {
     image = "ubuntu-os-cloud/ubuntu-1604-lts"
   }
   auto_delete = true
 }

// Make sure flask is installed on all new instances for later steps
 metadata_startup_script = "sudo apt update; sudo apt install -yq build-essential haveged rsync google-cloud-sdk openjdk-8-jre-headless"

 network_interface {
   network = "${google_compute_network.default.name}"

   access_config {
     // Include this section to give the VM an external ip address
   }
 }

 scheduling {
  preemptible = "false" // "true"
  automatic_restart = "false"
 }

  provisioner "file" {
    source      = "setup.sh"
    destination = "/tmp/setup.sh"

    connection {
      type = "ssh"
      user = "${var.ssh_user}"
    }
  }

  provisioner "file" {
    source      = "constellation.service"
    destination = "/tmp/constellation.service"

    connection {
      type = "ssh"
      user = "${var.ssh_user}"
    }
  }


  provisioner "file" {
    source      = "start"
    destination = "/tmp/start"

    connection {
      type = "ssh"
      user = "${var.ssh_user}"
    }
  }

  provisioner "file" {
    source      = "dag"
    destination = "/tmp/dag"

    connection {
      type = "ssh"
      user = "${var.ssh_user}"
    }
  }

 provisioner "remote-exec" {
  inline = [
    "chmod +x /tmp/setup.sh",
    "/tmp/setup.sh"
  ]
  connection {
    type = "ssh"
    user = "${var.ssh_user}"
  }
 }

  provisioner "remote-exec" {
  inline = [
    "sudo systemctl start constellation"
  ]
  connection {
    type = "ssh"
    user = "${var.ssh_user}"
  }
 }

}

resource "google_compute_network" "default" {
  name = "dag-network"
}

resource "google_compute_firewall" "default" {
 name    = "constellation-app-firewall"
 network = "${google_compute_network.default.name}"

 // enable_logging = true

  allow {
    protocol = "icmp"
  }

 allow {
   protocol = "tcp"
   ports    = ["22", "9000","9001","9010", "9011"]
 }
}
