variable "nodes" {
  description = "The number of nodes to deploy"
  type        = number
  default     = "1"
}

variable "distro" {
  default = "fedora-31"
}

variable instance_type {
  default = "i3.large"
}

variable "public_key_path" {}

variable "distro_ami" {
  type = map(string)
  default = {
    "fedora-31"      = "ami-0e82cc6ce8f393d4b"
    "ubuntu-bionic"  = "ami-0dd655843c87b6930"
    "rhel-8"         = "ami-00896a8434a915866"
    "amazon-linux-2" = "ami-024c80694b5b3e51a"
  }
}

variable "distro_ssh_user" {
  type = map(string)
  default = {
    "fedora-31"      = "fedora"
    "ubuntu-bionic"  = "ubuntu"
    "rhel-8"         = "ec2-user"
    "amazon-linux-2" = "ec2-user"
  }
}
