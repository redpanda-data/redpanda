output "ip" {
  value = aws_instance.node.*.public_ip
}

output "private_ips" {
  value = aws_instance.node.*.private_ip
}

output "ssh_user" {
  value = var.distro_ssh_user[var.distro]
}
