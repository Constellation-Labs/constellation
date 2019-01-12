variable "random_id" {
  type = "string"
}

resource "google_compute_network" "default" {
  name = "dag-network-${var.random_id}"
}

output "network_name" {
  value = "${google_compute_network.default.name}"
}

resource "google_compute_firewall" "default" {
 name    = "dag-firewall-${var.random_id}"
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