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

provider "aws" {
  profile = "default"
  region  = "us-west-2"
}

resource "aws_instance" "node" {
  count                  = var.nodes
  ami                    = var.distro_ami[var.distro]
  instance_type          = var.instance_type
  key_name               = aws_key_pair.ssh.key_name
  vpc_security_group_ids = [aws_security_group.node_sec_group.id]
  tags = {
       owner: "${var.owner}-${local.deployment_id}"
  }

  connection {
    user        = var.distro_ssh_user[var.distro]
    host        = self.public_ip
    private_key = file(var.private_key_path)
  }
}

resource "aws_security_group" "node_sec_group" {
  name        = "${var.owner}-${local.deployment_id}-node-sec-group"
  tags = {
       owner: "${var.owner}-${local.deployment_id}"
  }
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

  # HTTP access to the trogdor agent and controller ports
  ingress {
    from_port   = 8888
    to_port     = 8889
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # grafana
  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # prometheus
  ingress {
    from_port   = 9090
    to_port     = 9090
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
  key_name   = "${var.owner}-${local.deployment_id}-key"
  public_key = file(var.public_key_path)
}
