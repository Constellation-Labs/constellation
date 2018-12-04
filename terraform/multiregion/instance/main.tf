output "instance_ips" {
  value = "${google_compute_instance.default.*.network_interface.0.access_config.0.assigned_nat_ip}"
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

variable "network_name" {
  type = "string"
}

variable "random_id" {
  type = "string"
}

// Terraform plugin for creating random ids
resource "random_id" "instance_id" {
 byte_length = 4
}

// A single Google Cloud Engine instance
resource "google_compute_instance" "default" {
 count          = "${var.instance_count}"
 name = "constellation-${var.random_id}-${random_id.instance_id.hex}-${count.index}"
 machine_type   = "n1-highcpu-4"
 zone           = "${var.zone}"
 can_ip_forward = true


 tags = ["constellation-vm-${var.random_id}", "constellation-${var.random_id}-${random_id.instance_id.hex}"]

 boot_disk {
   initialize_params {
     image = "ubuntu-os-cloud/ubuntu-1604-lts",
     size = 200
   }
   auto_delete = true
 }

// Make sure flask is installed on all new instances for later steps
 metadata_startup_script = "sudo apt update; sudo apt install -yq build-essential haveged rsync google-cloud-sdk openjdk-8-jre-headless"

 network_interface {
   network = "${var.network_name}"

   access_config {
     // Include this section to give the VM an external ip address
   }
 }

 scheduling {
  preemptible = "false" // "true"
  automatic_restart = "false"
 }

  provisioner "remote-exec" {
  inline = [
    "echo 'worked' > abc"
  ]
  connection {
    type = "ssh"
    user = "${var.ssh_user}"
    timeout = "60s"
  }
 }

  provisioner "file" {
    source      = "setup.sh"
    destination = "/tmp/setup.sh"

    connection {
      type = "ssh"
      user = "${var.ssh_user}"
      timeout = "30s"
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
