This test installs the operator with the configurator flags set to use an older version of the configurator and the matching version of Redpanda.

It then upgrades the configurator image and tag to point to the current dev image.

The statefulset and all the pods should end up with the new configurator and a functioning Redpanda cluster.