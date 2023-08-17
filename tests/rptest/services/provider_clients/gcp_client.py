class GCPClient:
    """
    GCP Client class.
    Should implement function similar to AWS EC2 client
    """
    def __init__(self,
                 config,
                 logger,
                 endpoint=None,
                 disable_ssl=True) -> None:
        self._cli = self._make_client()

    def _make_client(self):
        return None

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
