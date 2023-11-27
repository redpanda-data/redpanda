from rptest.services.redpanda_cloud import CLOUD_TYPE_BYOC, CLOUD_TYPE_FMC
from rptest.services.redpanda_cloud import PROVIDER_AWS, PROVIDER_GCP


class cloudv2_object_store_blocked:
    def __init__(self, rp, logger):
        self.logger = logger
        if rp._cloud_cluster.config.provider == PROVIDER_AWS:
            self._delegate = cloudv2_object_store_blocked_aws(rp, logger)
        elif rp._cloud_cluster.config.provider == PROVIDER_GCP:
            self._delegate = cloudv2_object_store_blocked_gcp(rp, logger)
        else:
            raise RuntimeError(
                f"Given provider ({rp._cloud_cluster.config.provider}) not "
                "supported in object storage blocking")

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
        cluster_id = rp._cloud_cluster.cluster_id
        network_id = rp._cloud_cluster.current.network_id
        self._cloud_provider_client = rp._cloud_cluster.provider_cli
        # This is different for BYOC and FMC
        if rp._cloud_cluster.config.type == CLOUD_TYPE_BYOC:
            self._vpc_id = self._cloud_provider_client.get_vpc_by_network_id(
                network_id)['VpcId']
        elif rp._cloud_cluster.config.type == CLOUD_TYPE_FMC:
            _net = rp._cloud_cluster._get_network()
            _info = _net['status']['created']['providerNetworkDetails'][
                'cloudProvider'][rp._cloud_cluster.config.provider.lower()]
            self._vpc_id = _info['vpcId']
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
        self._cluster_id = rp._cloud_cluster.cluster_id
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
