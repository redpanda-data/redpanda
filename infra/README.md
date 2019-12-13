## Infra

This is a subproject containing infrastructure definitions. 

- [cluster](cluster/cluster.tf): A terraform plan to deploy a redpanda cluster
  (currently 1 node) on AWS

### Set up

If it's your first time deploying a module, running `vtools infra deploy` will
also install all the dependencies (AWS CLI v2 and Terraform), and ask you to
configure the AWS CLI, so that Terraform can use your access key to deploy the
modules. To create a Secret Access Key, follow
[this guide](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey)


### Deploying a module

You can use [vtools infra](../tools/vtools/infra/commands.py) to deploy the
modules. For example, to deploy the `cluster` module, run:

```sh
vtools infra deploy \
  --v-root <path to v repo> \
  --module cluster
```

To destroy it:

```sh
vtools infra destroy \
  --v-root <path to v repo> \
  --module cluster
```

To override a module variable, pass it in the form of a key-value pair
(`key=value`):

```sh
vtools infra deploy \
  --v-root <path to v repo> \
  --module cluster \
  distro=ubuntu-bionic instance_type=i3.large
```

To see the module's available variables, please see its `vars.tf` file.

To avoid having to pass the module's variables to create it and destroy it, you
can create a file called `terraform.tfvars` in the module's directory, and write
one `key = value` pair per line. Terraform will load them automatically.

Alternatively, you can omit the file and key-value pairs, and enter the required
variable values interactively when prompted.
