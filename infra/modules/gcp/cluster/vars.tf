variable "region" {
  default = "us-west1"
}

variable "zone" {
  description = "The zone where the cluster will be deployed [a,b,...]"
  default     = "a"
}

variable "owner" {
  description = "Your vectorized username."
}

variable "nodes" {
  description = "The number of nodes to deploy."
  type        = number
  default     = "1"
}

variable "disks" {
  description = "The number of local disks on each machine."
  type        = number
  default     = 1
}

variable "image" {
  # See https://cloud.google.com/compute/docs/images#os-compute-support
  # for an updated list.
  default = "ubuntu-os-cloud/ubuntu-1804-lts"
}

variable machine_type {
  # List of available machines per region/ zone:
  # https://cloud.google.com/compute/docs/regions-zones#available
  default = "n2-standard-2"
}

variable "public_key_path" {
  description = "The ssh key. Its user should match 'owner'."
}
