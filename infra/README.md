## Infra

This is a subproject containing infrastructure definitions. 

- [cluster](cluster/cluster.tf): A terraform plan to deploy a redpanda cluster
  (currently 1 node) on AWS

### Set up

If it's your first time running `deploy.py`, it will also install all the
dependencies (AWS CLI v2 and Terraform), and ask you to configure the AWS CLI,
so that Terraform can use your access key to deploy the plans. To create
a Secret Access Key, follow
[this guide](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey)


### Deploying a plan

You can use [deploy.py](deploy.py) to deploy the modules. For example, to deploy
the `cluster` module, run:

```sh
./deploy.py        \
  --action apply   \
  --module cluster
```

To destroy it:

```sh
./deploy.py        \
  --action destroy \
  --module cluster
```

To override a module variable, pass a `--var` flag:

```sh
./deploy.py        \
  --action destroy \
  --module cluster \
  --var distro=ubuntu-bionic instance_type=i3.large
```

To see the module's available variables, please see its `vars.tf` file.

To avoid having to pass the module's variables to create it and destroy it, you
can create a file called `terraform.tfvars` in the module's directory, and write
one `key = value` pair per line. Terraform will load them automatically.

Alternatively, you can omit the file and `--var`, and enter the required variable
values interactively.
