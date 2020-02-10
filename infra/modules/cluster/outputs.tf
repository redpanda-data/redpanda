output "ip" {
  value = aws_eip.elastic_ip.*.public_ip
}

output "ssh_cmd" {
  value = formatlist("ssh -F ${abspath(dirname("."))}/${local.ssh_config_file} ${var.distro_ssh_user[var.distro]}@%s", aws_eip.elastic_ip.*.public_ip)
}

output "ssh_user" {
      value = var.distro_ssh_user[var.distro] 
}