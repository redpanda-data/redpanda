resource "random_uuid" "cluster" {}

locals {
  timestamp = timestamp()
}

locals {
  uuid = random_uuid.cluster.result
}

locals {
  deployment_id = "${random_uuid.cluster.result}-${local.timestamp}"
}

locals {
  ssh_config_file = "ssh_config-${local.uuid}"
}

provider "aws" {
  profile = "default"
  region  = "us-west-1"
}

resource "aws_eip_association" "eip_assoc" {
  count         = var.nodes
  instance_id   = aws_instance.node[count.index].id
  allocation_id = aws_eip.elastic_ip[count.index].id
}

resource "aws_eip" "elastic_ip" {
  count = var.nodes
  vpc   = true
}

resource "aws_instance" "node" {
  count                  = var.nodes
  depends_on             = [aws_eip.elastic_ip]
  ami                    = var.distro_ami[var.distro]
  instance_type          = var.instance_type
  key_name               = aws_key_pair.ssh.key_name
  vpc_security_group_ids = [aws_security_group.node_sec_group.id]

  connection {
    user        = var.distro_ssh_user[var.distro]
    host        = self.public_ip
    private_key = file(var.private_key_path)
  }

  provisioner "local-exec" {
    command = "echo 'IdentityFile ${var.private_key_path}\nCompression yes' > ${local.ssh_config_file}"
  }

  provisioner "local-exec" {
    environment = {
      PKG_PATH        = var.local_package_abs_path
      SSH_KEY         = var.private_key_path
      SSH_USER        = var.distro_ssh_user[var.distro]
      SSH_CONFIG_FILE = local.ssh_config_file
      IP              = self.public_ip
      TIMEOUT         = var.ssh_timeout
      RETRIES         = var.ssh_retries
    }

    command = "./scp_local_pkg.sh"
  }

  provisioner "file" {
    source      = "init.sh"
    destination = "/tmp/init.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "set -ex",
      "chmod +x /tmp/init.sh",
      "/tmp/init.sh ${var.packagecloud_token}",
      "sudo rpk config set id ${count.index}",
      "sudo rpk config set seed-nodes --hosts ${join(",", aws_eip.elastic_ip.*.public_ip)}",
      "sudo systemctl start redpanda-tuner",
      "sudo systemctl start redpanda"
    ]
  }

  provisioner "local-exec" {
    when       = destroy
    on_failure = continue
    command    = "rm ${local.ssh_config_file}"
  }
}

resource "aws_security_group" "node_sec_group" {
  name        = "node-sec-group-${local.deployment_id}"
  description = "redpanda ports"

  # SSH access from anywhere
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTP access from anywhere to port 9092
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTP access to the RPC port
  ingress {
    from_port   = 33145
    to_port     = 33145
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTP access to the Admin port
  ingress {
    from_port   = 9644
    to_port     = 9644
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "ssh" {
  key_name   = "key-${local.deployment_id}"
  public_key = file(var.public_key_path)
}
