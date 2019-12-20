# Cluster

A [Terraform module](cluster.tf) to deploy a redpanda cluster. It launches a
configurable number of VMs of the specified type into the default VPC, with a
security group allowing TCP connections to ports 22 (SSH) and 9092 (redpanda),
33145 (RPC) and 9644 (Admin), and all outbound traffic.

If a path to a local RPM or DEB redpanda package is passed, it will scp it into
the VMs, install it and start it with systemd. Otherwise, it configures the
packagecloud repo in the machine, installs the latest redpanda package for the
current distro and starts it.

### [Variables](vars.tf)
- `nodes`: The number of nodes to deploy.

  Default: 1

- `distro`: The distribution to use for the VM.
  
  Default: `fedora-31`
  
  Supported: `fedora-31`, `amazon-linux-2`, `rhel-8`, `ubuntu-bionic`

- `instance_type`: The EC2 instance type.

  Default: `i3.large`

- `local_package_abs_path`: Optional. The absolute path to a local package to
  deploy and install into to the VMs.

- `ssh_timeout`: The timeout (in seconds) for establishing an SSH connection to
  the created VMs, while copying the local package file when
  `local_package_abs_path` is set.
  
  Default: 30 seconds
  
- `ssh_retries`: The number of retries to attempt to establish an SSH connection
  to the created VMs, while copying the local package file when
  `local_package_abs_path` is set.
  
  Default: 3

- `packagecloud_token`: The packagecloud repo token for vectorized. Required if
  `local_package_abs_path` isn't set.

- `private_key_path`: The private key to use to SSH into the created VMs.

- `public_key_path`: The public key to install in the server, to SSH into the
  created VMs.

### [Outputs](outputs.tf)
- `ip`: The VMs' public IPs.
- `ssh_cmd`: The commands to SSH into each created VM.
