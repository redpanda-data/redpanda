output "ip" {
  value = aws_instance.node.*.public_ip
}

output "private_ips" {
  value = aws_instance.node.*.private_ip
}

output "ssh_cmd" {
  value = formatlist("ssh -F ${local.ssh_config_file} ${var.distro_ssh_user[var.distro]}@%s", aws_instance.node.*.public_ip)
}

output "ssh_user" {
  value = var.distro_ssh_user[var.distro]
}

output "ducktape" {
  value = {
    "nodes" = [
      for n in aws_instance.node: {
        "externally_routable_ip" = n.public_ip,
        "ssh_config" = {
          "host" = n.public_ip,
          "hostname" = n.public_ip,
          "identityfile" = "/root/.ssh/infra-key",
          "password" = "",
          "port" = 22,
          "user" = "root"
        }
      }
    ]
  }
}
