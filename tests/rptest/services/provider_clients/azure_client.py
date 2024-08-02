from rptest.services.provider_clients.client_utils import query_instance_meta

headers = {"Metadata-Flavor": "Microsoft"}


class AzureClient:
    """
    Azure Client class.
    Should implement function similar to AWS EC2 client
    """
    def __init__(self,
                 region,
                 key,
                 secret,
                 tenant,
                 logger,
                 endpoint=None,
                 disable_ssl=True):

        self._region = region
        self._access_key = key
        self._secret_key = secret
        self._tenant = tenant
        self._endpoint = endpoint
        self._disable_ssl = disable_ssl
        self._cli = self._make_client()

    def _make_client(self):
        # TODO: Work on Azure client
        return None

    @property
    def project_id(self):
        return "devprod-cicd-infra"  # HACK

    def create_vpc_peering(self, params, facing_vpcs=True):
        """
        Creates two peering connections facing each other
        """
        return "hardcode"  # HACK

    def get_vpc_by_network_id(self, network_id, prefix=None, project_id=None):
        """
        Get VPC Network
        """
        return "hardcode"  # HACK

    def find_vpc_peering_connection(self, state, params):
        return None

    def accept_vpc_peering(self, peering_id, dry_run=False):
        return None

    def get_vpc_peering_status(self, vpc_peering_id):
        return None

    def get_route_table_ids_for_cluster(self, cluster_id):
        return None

    def get_route_table_ids_for_vpc(self, vpc_id):
        return None

    def create_route(self, rtb_id, destCidrBlock, vpc_peering_id):
        return None

    def get_single_zone(self, region):
        """
        Get list of available zones based on region
        Example zone item returned by list:
        """
        return f"{region}-az2"  # HACK, hardcoded

    def get_instance_meta(self, target='localhost'):
        """
        Query meta from local node.
        Source: https://cloud.google.com/compute/docs/metadata/querying-metadata
        """
        # prepare request data
        _prefix = "http://"
        _suffix = "/computeMetadata/v1/instance/"
        _target = "169.254.169.254" if target == 'localhost' else target
        uri = f"{_prefix}{_target}{_suffix}"

        return query_instance_meta(uri, headers=headers)

    def block_bucket_access(self, cluster_id, bucket_name):
        return False

    def unblock_bucket_access(self, cluster_id, bucket_name, binding):
        """
        Add binding to bucket iam policy
        """
        pass

    def list_buckets(self):
        raise NotImplementedError("Bucket operations is not supported for GCP")

    def delete_buckets(self):
        raise NotImplementedError("Bucket operations is not supported for GCP")