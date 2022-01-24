---
title: Version Upgrade
order: 0
---

## Version Upgrade

As part of Redpanda's evolution, new versions are released over time to deliver new features and improvements.
Due to that, it's always important to keep Redpanda up to date. This guide describes how to perform the upgrade depending on the environment that Redpanda is running.

** It's always important to read the [release notes](https://github.com/vectorizedio/redpanda/releases) before upgrading to different versions to anticipate and deal with any breaking changes. **

# Checking current version

Before changing to a different version, it is important to check which version you are currently running on your environment.
For that you can simply call the rpk command as shown below:
```bash
rpk version
```

The output should be similar to:
```bash
v21.11.2 (rev f58e69b)
```

# Kubernetes
As YAML files are the base for Kubernetes setup, changing the file is the easiest way to achieve this. Remember to apply the `version` tag for the latest version: 
```yaml
...
spec:
  image: "vectorized/redpanda"
  version: "latest"
...
```

After you save the file, apply the changes with the command:
```bash
kubectl apply -f <file_path/file_name>
```

Once the changes are applied, the cluster will restart and the changes can be validated with the `describe` command:
```bash
kubectl describe pod <pod_name>
```

# Docker
As docker relies on images, replace the current image value with a new one.
Initially, check which image version that is running on your docker:
```bash
docker images
```

After running the command you should be able to see the list of all images, including Redpanda's one. Now it is time to check which version on the [release notes](https://github.com/vectorizedio/redpanda/releases) page.

Before starting the upgrade, you might want to backup Redpanda's configuration file that is currently running to avoid any losses.

```bash
docker cp <containerId>:/etc/redpanda/redpanda.yaml <preferred location>
```

Let's make sure that there is no Redpanda container up and running:

```bash
docker ps
```

Stop and remove Redpanda's container(s):

```bash
docker stop <container_id>
...
docker rm <container_id>
```

Remove current images:

```bash
docker rmi <image_id>
```

The next step is to pull the desired Redpanda's version, or you can simply set 'latest' in the 'version' tag:
```bash
docker pull docker.vectorized.io/vectorized/redpanda:<version>
```

Once the image is downloaded, the steps described in the [docker getting started document](./quick-start-docker.md) can be followed to make Redpanda available once again.
If previous configuration were backed up, make sure to apply the `redpanda.yaml` file back to the configuration folder `/etc/redpanda` inside the container. After that, restart the cluster:
```bash
docker cp <path_to_saved_yaml_file>/redpanda.yaml <container_id>:/etc/redpanda
...
docker restart <container_name>
```

Now you have the desired version of Redpanda running.

# Linux
For Linux distributions, the process is simple, but it changes according to the distribution:
- On Fedora/RedHat systems:
    Execute the following commands on the terminal:
    ```bash
        sudo yum update redpanda
    ```

- On Debian/Ubuntu systems:
    Execute the following commands on the terminal:
    ```bash
        sudo apt-get --only-upgrade install redpanda
    ```

    or
    ```bash
        sudo apt-get install redpanda
    ```

    Once the upgrade is complete, you can perform the following check to validate the action:
    ```bash
        rpk --version
    ```

# MacOS
For MacOS, if Redpanda's installation was performed through brew, the upgrade can be applied by running the following command:
```bash
brew install vectorizedio/tap/redpanda
```

If the installation was executed from a binary file, download the preferred version from the [release list](https://github.com/vectorizedio/redpanda/releases), and overwrite the current rpk file in the installed location.
 

# Windows
On Windows it is only possible to run Redpanda on Docker. Therefore, the [Docker guide](#docker) is applicable for it.

# Post-upgrade actions
Once the upgrade is done, you need to make sure that the cluster is healthy.
To check if the cluster is properly running, use this command :
```bash
rpk cluster info
```

or
```bash
rpk cluster status
```

It is also important to keep closer monitoring, which can be done by following our [monitoring guide](./monitoring.md).

# Downgrade
If, for any reason, you have faced issues during the upgrade, like breaking changes, restoring to the previous version will guarantee that your downtime won't be affected more than expected.
Make sure to check what is your [current version](#checking-current-version) before starting the upgrade, and replace the configuration according to your running environment.
This guide is focused on Redpanda last version upgrade, so if you want to use a specific version, please check our [releases page](https://github.com/vectorizedio/redpanda/releases) to refer to your upgrade.