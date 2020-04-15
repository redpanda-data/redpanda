variable "nodes" {
  description = "The number of nodes to deploy"
  type        = number
  default     = "1"
}

variable "owner" {
  description = "Your vectorized username."
}

variable "distro" {
  default = "debian-stretch"
}

variable instance_type {
  default = "i3.large"
}

variable "public_key_path" {}

variable "distro_ami" {
  type = map(string)
  default = {
    # https://wiki.debian.org/Cloud/AmazonEC2Image/Stretch
    "debian-stretch" = "ami-0d270a69ac13b22c3"

    # https://alt.fedoraproject.org/cloud/
    "fedora-31" = "ami-0e82cc6ce8f393d4b"

    # https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#LaunchInstanceWizard:
    "ubuntu-bionic"  = "ami-09b69ac16c0287f96"
    "rhel-8"         = "ami-087c2c50437d0b80d"
    "amazon-linux-2" = "ami-0d6621c01e8c2de2c"
  }
}

variable "distro_ssh_user" {
  type = map(string)
  default = {
    "debian-stretch" = "admin"
    "fedora-31"      = "fedora"
    "ubuntu-bionic"  = "ubuntu"
    "rhel-8"         = "ec2-user"
    "amazon-linux-2" = "ec2-user"
  }
}