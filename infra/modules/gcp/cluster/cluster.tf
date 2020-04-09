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
  ssh_config_file = "${abspath(dirname("."))}/ssh_config-${local.uuid}"
}

provider "google" {
  credentials = file(pathexpand("~/.gcp.json"))
  project = "vectorized"
  region  = var.region
  zone    = "${var.region}-${var.zone}"
}

resource "google_compute_instance" "node" {
  count        = var.nodes
  name         = "${var.owner}-${local.uuid}-${count.index}"
  # The "test-clusters" VPC has specific firewall rules for nodes tagged with
  # "rp-node", allowing ingress traffic only on ports 22, 3000, 9090, 9092, 33145, 
  # 9644, 8888 and 8889.
  # https://console.cloud.google.com/networking/firewalls/details/test-clusters-rp-node-firewall?project=vectorized
  tags = ["rp-node"]
  machine_type = var.machine_type

  metadata = {
    ssh-keys = <<KEYS
${var.owner}:${file(abspath(var.public_key_path))}
KEYS
  }

  boot_disk {
    initialize_params {
      image = var.image
    }
  }

  scratch_disk {
    // 375 GB local SSD drive. 
    interface = "NVME"
  }

  network_interface {
    # test-clusters is an existing dedicated VPC for test cluster deployments
    # (https://console.cloud.google.com/networking/networks/list?project=vectorized).
    # VPN "default" must not be used since firewall rules are applied VPC-wide,
    # not per-machine like in AWS.
    # Additionally, we shouldn't create a new VPC per deployment, since they're
    # limited (default is 15 max).
    subnetwork = "test-cluster-subnet-1"
    access_config {
    }
  }
}
