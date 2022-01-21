---
title: Version Upgrade
order: 0
---

## Version Upgrade

As part of Redpanda's evolution, new versions are released over time in order to deliver new features and improvements.
Due to that it's always important to keep Redpanda up to date. This guide describes how to perform the upgrade depending on the environment that Redpanda is running.

** It's always important to read the [release notes](https://github.com/vectorizedio/redpanda/releases) before upgrading to different versions in order to anticipate and deal with any breaking changes. **

# Checking current version

Before changing to different version, it is important to check which version that you are currently running on your environment.
For that you can simply call the rpk command as shown below:
```bash
rpk version
```

The output should be similar to:
```bash
v21.11.2 (rev f58e69b)
```

# Kubernetes
As YAML files are the base for Kubernetes setup, ideally to change the configuration the easiest way to change achieve this is to change the image version. Remember to change the `version` tag  for the latest version. For example, let's use `version 21.11.2`: 
```yaml
...
spec:
  image: "vectorized/redpanda"
  version: "latest"
...
```
On the other, if the desired version is a specific one, make sure that the version matches one of the items in the [release notes](https://github.com/vectorizedio/redpanda/releases) and apply like the following:
```yaml
...
spec:
  image: "vectorized/redpanda"
  version: "v21.11.2"
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
As docker relies on images, the upgrade relies on replacing the current image to a new one.
Initially it is important to check which image version that is running on your docker, and for that on a terminal or CMD execute the following command:

```bash
docker images
```

After running the commanda you should be able to see the list of all images, including Redpanda's one. Now it is time to check which version on the [release notes](https://github.com/vectorizedio/redpanda/releases) page.

Before start the upgrade, you might want to copy the configuration file from Redpanda that is currently running, in order to avoid setup from scratch.

```bash
docker cp <containerId>:/etc/redpanda/redpanda.yaml <preferred location>
```

Let's make sure that there is no redpanda container up and running:

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

Next step is to pull the desired Redpanda's version, or if the preferred one is the latest one you can set 'latest' in 'version' tag:
```bash
docker pull docker.vectorized.io/vectorized/redpanda:<version>
```

Once the image is downloaded, the steps described in the [docker getting started document](./quick-start-docker.md) can be followed to make Redpanda available once again.
If previous configuration were saved, make sure to copy the redpanda.yaml file back to the /etc/redpanda container's folder and restart the cluster:
```bash
docker cp <path_to_saved_yaml_file>/redpanda.yaml <container_id>:/etc/redpanda
...
docker restart <container_name>
```

Now you have the desired version of Redpanda running.

# Linux
For Linux distributions, the process is really simplified but it changes depending on the distribution:
- On Fedora/RedHat systems:
    Execute the following commands on terminal:
    ```bash
        sudo yum update redpanda
    ```
- On Debian/Ubuntu systems:
    Execute the following commands on terminal:
    ```bash
        sudo apt-get --only-upgrade install redpanda
    ```
    or
    ```bash
        sudo apt-get install redpanda
    ```
    Once the upgrade is done, you can perform the following check to validate the upgrade:
    ```bash
        rpk --version
    ```

# MacOS
For MacOS, if Redpanda's installation was performed through brew, the upgrade can be achieved by running the following command:
```bash
brew install vectorizedio/tap/redpanda
```
If the installing was executed from a binary file, download the preferred version from the [rpk archive](https://github.com/vectorizedio/redpanda/releases), and overwrite the current rpk file in the installed location.
 

# Windows
On Windows it is only possible to run Redpanda on Docker. Therefore, the [Docker guide](#docker) is applicable for Windows OS.

# Post-upgrade actions
Once the upgrade is done, you need to make sure that cluster is healthy.
In order to check if the cluster is properly running, use command :
```bash
rpk cluster info
```
or
```bash
rpk cluster status
```
It is also important to keep a closer monitoring, which can be done by following our [monitoring guide](./monitoring.md).

# Downgrade
If for any reason you have faced issues during the upgrade, like breaking changes, restoring to the previous version will guarantee that your downtime won't be affected more than expected.
Make sure of [checking current version](#checking-current-version) before start the upgrade, and replace the configuration depending on the environment that Redpanda is running, with the desired 
All upgrade topics above are intended to upgrade to the latest version, some are explicity written, others simply ask to update, so be aware of these changes during the downgrade.