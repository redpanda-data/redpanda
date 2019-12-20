output "ip" {
  value = aws_instance.node.public_ip
}

output "ssh_cmd" {
  value = "ssh -F ${abspath(dirname("."))}/ssh_config ${var.distro_ssh_user[var.distro]}@${aws_instance.node.public_ip}"
}
