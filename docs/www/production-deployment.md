---
title: Production Deployment
order: 0
---
# Production Deployment

This guide will take you through what is needed to setup a production cluster
of Redpanda.

### Requirements

For the best performance the following are strongly recommended:

- XFS for the data directory of Redpanda (/var/lib/redpanda/data)
- A kernel that is at least 3.10.0-514, but a 4.18 or newer kernel is preferred
- Local NVMe, RAID-0 when using multiple disks
- 2GB of memory per core
- The following ports need to be opened
  - `33145` - Internal RPC Port
  - `9092` - Kafka API Port
  - `9644` - Prometheus & HTTP Admin port

## Installation

First we need to provision the hardware needed. This can be done [manually](#manual-installation) or with the provided Terraform deployment files found here: [https://github.com/vectorizedio/deployment-automation].

First, install Terraform using the [Terraform Installation Tutorial](https://learn.hashicorp.com/tutorials/terraform/install-cli).
Then clone the `deployment-automation` repo:

```
git clone git@github.com:vectorizedio/deployment-automation.git
```

Then change into the directory:

```
cd deployment-automation
```

From here you can follow the specific instructions for [AWS](#aws) or
[GCP](#gcp)

### AWS

Within the `deployment-automation` folder change into the `aws` directory

```
cd aws
```

#### Step 1: Set AWS Credentials

First we need to set the AWS secret and key. Terraform provide multiple ways
to set this which is covered here: [https://registry.terraform.io/providers/hashicorp/aws/latest/docs#environment-variables]

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
terraform apply -var="instance_type=i3.large" -var="nodes 3"
```

### GCP

Within the `deployment-automation` folder change into the `gcp` directory

```
cd gcp
```

#### Step 1: Prerequisites

You will need an existing subnet to deploy the VMs into. The subnet's attached
firewall should allow inbound traffic on ports 22, 3000, 8888, 8889, 9090,
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

## Install Redpanda

Once the hardware has been provisioned. You can choose to either install
Redpanda using our provided Ansible Playbooks or install Redpanda manually.

- [Installation with Ansible](#Installation-with-Ansible)
- [Manual Installation](#Manual-Installation)

## Installation with Ansible

### Requirements

- Install Ansible - https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html

### Step 1: Clone the Github Repo

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
It will look something like:

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

### Step 3: Run the Ansible Playbook

You can now setup Redpanda on your selected nodes by running the following
command:

```
ansible-playbook --private-key <your_private_key> -i hosts.ini -v ansible/playbooks/provision-node.yml
```

Once this completes you will have a fully running cluster.

## Manual Installation

### Step 1: Install the Binary

On Fedora/RedHat Systems:

```
curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.rpm.sh' | sudo -E bash && sudo yum install redpanda -y 
```

On Debian Systems:

```
curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash && sudo apt install redpanda -y
```

### Step 2: Set Redpanda Production Mode

By default Redpanda is installed in **Development** mode, to we next need to
set Redpanda to run in **Production** mode. This is done with:

```
sudo rpk mode production
```

We then need to tune the hardware, which can be done by running the following
on each node:

```
sudo rpk tune all
```

> **_Optional: Benchmark your SSD_**
> 
> On taller machines we recommend benchmarking your SSD. This can be done
> with `rpk iotune`. You only need to run this once. For reference, a decent
> local  NVMe SSD should yield around 1GB/s sustained writes.
> `rpk iotune` will capture SSD wear and tear and give accurate measurements
> of what your hardware is actually capable of delivering. It is recommended
> you run this before benchmarking.
>
>If you are on AWS, GCP or Azure, creating a new instance and upgrading to
>an image with a recent Linux Kernel version is often the easiest way to
>work around bad devices.
>
>```
>sudo rpk iotune # takes 10mins
>```

### Step 3: Configure and Start the Root Node

Now that the software is installed we need to configure it. The first step is
to setup the root node. The root node will start as a standalone node, and
every other one will join it, forming a cluster along the way.

For the root node we’ll choose 0 as its ID. --self tells the node which interface address to bind to. Usually you want that to be its private IP.

```
sudo rpk config bootstrap --id 0 --self <ip> && \
sudo systemctl start redpanda-tuner redpanda
```

### Step 4: Configure and Start the Rest of the Nodes

For every other node, we just have to choose a unique integer id for it and let
it know where to reach the root node.

```
sudo rpk config bootstrap --id <unique id> \
--self <private ip>                        \
--ips <root node ip> &&                    \
sudo systemctl start redpanda-tuner redpanda
```

### Step 5: Verify Installation

You can verify that the cluster is up and running by checking the logs:

```
journalctl -u redpanda
```

You should also be able to create a topic with the following command:

```
rpk api topic create panda
```