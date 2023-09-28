class cloudv2_object_store_blocked:
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
