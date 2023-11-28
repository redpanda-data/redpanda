class CloudBroker():
    def __init__(self, pod, kubectl, logger) -> None:
        self.logger = logger
        # Validate
        if not isinstance(pod, dict) or pod['kind'] != 'Pod':
            self.logger.error("Invalid pod data provided")
        # Metadata
        self.operating_system = 'k8s'
        self._meta = pod['metadata']
        self.name = self._meta['name']
        self.slot_id = int(
            self._meta['labels']['operator.redpanda.com/node-id'])
        self.uuid = self._meta['uid']

        # Save other data
        self._spec = pod['spec']
        self._status = pod['status']

        # save client
        self._kubeclient = kubectl

        # Backward compatibility
        self.account = self._meta

    # Backward compatibility
    def ssh_output(self, cmd):
        return self._kubeclient.exec(cmd)
