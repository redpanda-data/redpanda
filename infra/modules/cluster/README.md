# Cluster

A [Terraform plan](cluster.tf) to deploy a redpanda cluster. It currently
launches a VM of the specified type into the default VPC, with a security group
allowing TCP connections to ports 22 (SSH) and 9092 (redpanda), and all
outbound traffic.

It then configures the packagecloud repo in the machine, installs the redpanda
package and starts its systemd service.

### [Variables](vars.tf)
- `distro`: The distribution to use for the VM.
  
  Default: `fedora-31`
  
  Supported: `fedora-31`, `amazon-linux-2`, `rhel-8`, `ubuntu-bionic`

- `instance_type`: The EC2 instance type.

  Default: `i3.large`

- `packagecloud_token`: The packagecloud repo token for vectorized.
- `pub_key_path`: The public key to use to SSH into the created VMs.

### [Outputs](outputs.tf)
- The VMs IPs.
