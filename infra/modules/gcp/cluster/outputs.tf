output "ip" {
  value = google_compute_instance.node.*.network_interface.0.access_config.0.nat_ip
}

output "private_ips" {
  value = google_compute_instance.node.*.network_interface.0.network_ip
}

output "ssh_user" {
  value = var.owner
}
