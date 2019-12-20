provider "aws" {
  profile = "default"
  region  = "us-west-1"
}

resource "aws_instance" "node" {
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
    environment = {
      PKG_PATH = var.local_package_abs_path
      SSH_KEY  = var.private_key_path
      SSH_USER = var.distro_ssh_user[var.distro]
      IP       = self.public_ip
      TIMEOUT  = var.ssh_timeout
      RETRIES  = var.ssh_retries
    }

    command = "./scp_local_pkg.sh"
  }

  provisioner "file" {
    source      = "init.sh"
    destination = "/tmp/init.sh"
  }

  provisioner "remote-exec" {
    inline = [
      "chmod +x /tmp/init.sh",
      "/tmp/init.sh ${var.packagecloud_token}",
    ]
  }
}

resource "aws_security_group" "node_sec_group" {
  name        = "node-sec-group"
  description = "SSH and Kafka ports"

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

  # outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "ssh" {
  key_name   = "key"
  public_key = file(var.public_key_path)
}
