---
title: What is next?
order: 0
---
# What is next?

Now that Redpanda is properly running in your environment, there is a lot of features and configuration that can be done to your instance.

Create an application to interact with the cluster might help to understand how Redpanda works, for that our [NodeJS with Redpanda](./guide-nodejs.md) is the first step to interact with the topics.

It's recommended to add an additional layer of security to your environment. Applying features like [authorization and authentication](./acls.md) or [TLS encryption for Kubernetes](./tls-kubernetes.md) is highly recommended, specially in a production environment. It is important to note that changes to configuration files, not only from a local machine, but also for Kubernetes and Docker requires restart of the cluster.

For Kubernetes, it is necessary to apply the changes from the updated yaml file, for that use the command below:
```bash
kubectl apply -f <file_path/file_name>
```

For Docker, restarting the container will apply the changes made to Redpanda's cluster:
```bash
docker restart <container_name>
```

For more detailed information about the RPK commands, our [RPK commands overview](./rpk-commands.md) can help you to easily configure and interact with the topics.

Another great source of Redpanda's material can be found within our [blog](./https://vectorized.io/blog), not only to technical guides, but also Redpanda's announcements.