class cloudv2_object_store_blocked:
    def __init__(self, rp, logger):
        self.logger = logger
        if rp._cloud_cluster.config.provider == "aws":
            self._delegate = cloudv2_object_store_blocked_aws(rp, logger)
        else:
            self._delegate = cloudv2_object_store_blocked_gcp(rp, logger)

    def __enter__(self):
        """apply a blocking policy"""
        self._delegate.__enter__()

    def __exit__(self, type, value, traceback):
        """remove block policy"""
        self._delegate.__exit__(type, value, traceback)


class cloudv2_object_store_blocked_aws:
    """Temporary block writes to a cloudv2 TS bucket"""
    def __init__(self, rp, logger):
        self.logger = logger
        cluster_id = rp._cloud_cluster.config.id
        network_id = rp._cloud_cluster.current.network_id
        self._cloud_provider_client = rp._cloud_cluster.provider_cli
        self._vpc_id = self._cloud_provider_client.get_vpc_by_network_id(
            network_id)['VpcId']
        self._bucket_name = f'redpanda-cloud-storage-{cluster_id}'

    def __enter__(self):
        """apply a blocking policy"""
        self.logger.debug(f'Blocking access to {self._bucket_name}')
        self._cloud_provider_client.block_bucket_from_vpc(
            self._bucket_name, self._vpc_id)

    def __exit__(self, type, value, traceback):
        """remove block policy"""
        self.logger.debug(f'Unblocking access to {self._bucket_name}')
        self._cloud_provider_client.unblock_bucket_from_vpc(self._bucket_name)


class cloudv2_object_store_blocked_gcp:
    def __init__(self, rp, logger):
        self.logger = logger
        self._cluster_id = rp._cloud_cluster.config.id
        network_id = rp._cloud_cluster.current.network_id
        self._cloud_provider_client = rp._cloud_cluster.provider_cli
        self._bucket_name = f'redpanda-cloud-storage-{self._cluster_id}'

    def __enter__(self):
        """apply a blocking policy"""
        self.logger.debug(f'Blocking access to {self._bucket_name}')
        self._block_ctx = self._cloud_provider_client.block_bucket_access(
            self._cluster_id, self._bucket_name)

    def __exit__(self, type, value, traceback):
        """remove block policy"""
        self.logger.debug(f'Unblocking access to {self._bucket_name}')
        self._cloud_provider_client.unblock_bucket_access(
            self._cluster_id, self._bucket_name, self._block_ctx)
