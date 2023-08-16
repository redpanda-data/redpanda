import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class EC2Client:
    """
    This almost mimics S3Client from archival.
    """
    def __init__(self, config, logger, endpoint=None, disable_ssl=True):

        self._region = config['region']
        self._access_key = config['access_key']
        self._secret_key = config['secret_key']
        self._endpoint = endpoint
        self._disable_ssl = disable_ssl
        self._cli = self._make_client()
        self._log = logger

        self._log.debug(f"Created EC2 Client: {self._region}, {endpoint},"
                        f" key is set = {self._access_key is not None}")

    def _make_client(self):
        cfg = Config(region_name=self._region,
                     retries={
                         'max_attempts': 10,
                         'mode': 'adaptive'
                     })
        return boto3.client('ec2',
                            config=cfg,
                            aws_access_key_id=self._access_key,
                            aws_secret_access_key=self._secret_key,
                            endpoint_url=self._endpoint,
                            use_ssl=not self._disable_ssl)

    def find_vpc_peering_connection(self, state, region_id, cidr, rp_vpc_id,
                                    rp_owner_id, dt_vpc_id, dt_owner_id):
        """
        Identifies peering connections by comparing
        AcceptorVpcInfo and RequestorVpcInfo
        """
        # Put all info values to the set
        _source_info = set([
            state, region_id, cidr, rp_vpc_id, rp_owner_id, dt_vpc_id,
            dt_owner_id
        ])
        # Get all available peering connections
        _resp = self._cli.describe_vpc_peering_connections()
        _ids = []
        # iterate them to find ours
        for _pvpc in _resp['VpcPeeringConnections']:
            # get shortcuts
            _id = _pvpc['VpcPeeringConnectionId']
            _r = _pvpc['RequesterVpcInfo']
            _a = _pvpc['AccepterVpcInfo']
            # Check that this is our connection
            # 1. Check that its from same region
            if _r['Region'] != _a['Region']:
                continue
            else:
                _info = [
                    _pvpc['Status']['Code'], _r['Region'], _r['CidrBlock'],
                    _r['OwnerId'], _r['VpcId'], _a['OwnerId'], _a['VpcId']
                ]
                if _source_info == _info:
                    _ids.append(_id)
        if len(_ids) > 1:
            self._log.warning("Found multiple peering "
                              f"connections (cleaning?): {_ids}")
        elif len(_ids) < 1:
            self._log.error(
                f"No peering connections found for given info: {_source_info}")

        self._log.info(f"Using peering connection with id: '{_ids[0]}'")
        return _ids[0]

    def accept_vpc_peering(self, peering_id, dry_run=False):
        """
        Accepts peering
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2/client/accept_vpc_peering_connection.html#
        """
        return self._cli.accept_vpc_peering_connection(
            DryRun=dry_run, VpcPeeringConnectionId=peering_id)
