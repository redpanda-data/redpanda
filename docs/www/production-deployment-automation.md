---
title: Using Terraform and Ansible to deploy Redpanda
order: 0
---

# Using Terraform and Ansible to deploy Redpanda

You can easily deploy Redpanda for production on [bare-metal with the Redpanda installation binary](/docs/production-deployment).

If you use automation tools in your environment, like Terraform and Ansible, you can use those tools for your Redpanda production deployment.

## Setting up your infrastructure with Terraform

First, install Terraform using the [Terraform Installation Tutorial](https://learn.hashicorp.com/tutorials/terraform/install-cli).
Then clone the `deployment-automation` repo:

```
git clone git@github.com:vectorizedio/deployment-automation.git
```

Then change into the directory:

```
cd deployment-automation
```

From here, follow the specific instructions for [AWS](#AWS) or
[GCP](#GCP).

### AWS

Within the `deployment-automation` folder change into the `aws` directory

```
cd aws
```

#### Step 1: Set AWS Credentials

First we need to set the AWS secret and key. Terraform provide multiple ways
to set this which is covered here: https://registry.terraform.io/providers/hashicorp/aws/latest/docs#environment-variables

#### Step 2: Init Terraform

Before using Terraform to deploy the cluster we need to run the following:

```
terraform init
```

#### Step 3: Create the Cluster

We now can run `terraform apply` to create the cluster. There are a number of
configuration options which can be specified when creating the cluster:

- `aws_region`: The AWS region to deploy the infrastructure on. Default: `us-west-2`.
- `nodes`: The number of nodes to base the cluster on. Default: `1`.
- `enable_monitoring`: Will create a prometheus/grafana instance to be used for monitoring the cluster. Default: `true`.
- `instance_type`: The instance type which redpanda will be deployed on. Default: `i3.8xlarge`.
- `prometheus_instance_type`: The instance type which prometheus and grafana will deployed on. Default: `c5.2xlarge`.
- `public_key_path`: Provide the path to the public key of the keypair used to access the nodes. Default: `~/.ssh/id_rsa.pub`
- `distro`: Linux distribution to install (this settings affects the below variables). Default: `ubuntu-focal`
- `distro_ami`: AWS AMI to use for each available distribution.
These have to be changed with according to the chosen AWS region.
- `distro_ssh_user`: User used to ssh into the created EC2 instances.

The following is an example of creating a three node cluster using i3.large
instances.

```
terraform apply -var="instance_type=i3.large" -var="nodes=3"
```

### GCP

Within the `deployment-automation` folder change into the `gcp` directory

```
cd gcp
```

#### Step 1: Prerequisites

You will need an existing subnet to deploy the VMs into. The subnet's attached
firewall should allow inbound traffic on ports 22, 3000, 8082, 8888, 8889, 9090,
9092, 9644 and 33145. This module adds the `rp-node` tag to the deployed VMs,
which can be used as the target tag for the firewall rule.

#### Step 2: Init Terraform

Before using Terraform to deploy the cluster we need to run the following:

```
terraform init
```

#### Step 3: Create the Cluster

We now can run `terraform apply` to create the cluster. There are a number of
configuration options which can be specified when creating the cluster:

- `region` (default: `us-west-1`): The region to deploy the infrastructure on.
- `zone` (default: `a`): The region's zone to deploy the infrastructure on.
- `subnet`: The name of an existing subnet to deploy the infrastructure on.
- `nodes` (default: `1`): The number of nodes to base the cluster on. Keep in mind that one node is used as a monitoring node.
- `disks` (default: `1`): The number of **local** disks to deploy on each machine
- `image` (default: `ubuntu-os-cloud/ubuntu-1804-lts`): The OS image running on the VMs.
- `machine_type` (default: `n2-standard-2`): The machine type.
- `public_key_path`: Provide the path to the public key of the keypair used to access the nodes.
- `ssh_user`: The ssh user. Must match the one in the public ssh key's comments.

The following is an example of creating a three node cluster using the subnet
named `redpanda-cluster-subnet`

```
terraform apply -var nodes=3 -var subnet=redpanda-cluster-subnet -var public_key_path=~/.ssh/id_rsa.pub -var ssh_user=$USER
```

### Next steps

From here, you can install Redpanda either:

- With the [Redpanda installation binary](/docs/production-deployment)
- With Ansible

## Installing Redpanda with Ansible

Whether you are using [bare-metal servers](/docs/production-deployment) or Terraform, you can use Ansible to install Redpanda.

### Requirements

- Install Ansible - https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html

### Step 1: Clone the GitHub repository

First we need to clone the repo:

```
git clone git@github.com:vectorizedio/deployment-automation.git
```

Then change into the directory:

```
cd deployment-automation
```

You will want install the required roles needed by Ansible with the following
command:

```
ansible-galaxy install -r ansible/requirements.yml
```

### Step 2: Configure the hosts.ini file

In the deployment-automation directory you will find a file called `hosts.ini`.
If you used Terraform to deploy the instances this file will be updated
automatically. If you did not use Terraform you will have to update it
manually.

If you need to update it manually, first open the file and you will see something like:

```
[redpanda]
ip ansible_user=ssh_user ansible_become=True private_ip=pip id=0
ip ansible_user=ssh_user ansible_become=True private_ip=pip id=1

[monitor]
ip ansible_user=ssh_user ansible_become=True private_ip=pip id=1
```

Under the `[redpanda]` section, you will want to replace the following:

- `ip` - the public ip address of the machine
- `ansible_user` - the username for ansible to use to ssh to the machine
- `private_ip` - the private ip address of the machine, could be the same as the public ip address
- `id` - The node id of the Redpanda instance, this needs to be unique for each host

The `[monitor]` section is if you wish to have Prometheus and Grafana installed
on a give host. If you wish to not have this deployed then remove the
`[monitor]` section.

### Step 3: Run the Ansible playbook

You can now setup Redpanda on your selected nodes by running the following
command:

```
ansible-playbook --private-key <your_private_key> -i hosts.ini -v ansible/playbooks/provision-node.yml
```

Once this completes you will have a fully running cluster.
