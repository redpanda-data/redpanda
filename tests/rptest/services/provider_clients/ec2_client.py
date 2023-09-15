import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from rptest.services.provider_clients.client_utils import query_instance_meta

RESPONCE_META_LABEL = "ResponseMetadata"
VPC_PEERING_LABEL = "VpcPeeringConnection"
VPC_PEERING_ID_LABEL = "VpcPeeringConnectionId"
VPCS_PEERING_LABEL = "VpcPeeringConnections"
VPCS_LABEL = "Vpcs"
RTBS_LABEL = "RouteTables"
RTB_ID_LABEL = "RouteTableId"
AZS_LABEL = "AvailabilityZones"
AZ_ID_LABEL = "ZoneId"


class EC2Client:
    """
    This almost mimics S3Client from archival.
    """

    VPC_ID_LABEL = "VpcId"
    OWNER_ID_LABEL = "OwnerId"
    CIDR_LABEL = "CidrBlock"

    def __init__(self,
                 region,
                 key,
                 secret,
                 logger,
                 endpoint=None,
                 disable_ssl=True):

        self._region = region
        self._access_key = key
        self._secret_key = secret
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

    def create_vpc_peering(self, params):
        """
        Create VPC peering connection in AWS
        """
        _r = self._cli.create_vpc_peering_connection(
            PeerOwnerId=params.peer_owner_id,
            PeerVpcId=params.peer_vpc_id,
            VpcId=params.rp_vpc_id,
            PeerRegion=params.region,
        )
        # TODO: Validate results

        # Return
        return _r[VPC_PEERING_LABEL][VPC_PEERING_ID_LABEL]

    def get_vpc_by_network_id(self, network_id, prefix=None):
        """
        Get VPC from AWS using network id from CloudV2
        """
        # Create a filter to search for the vpc
        if prefix is None:
            _name = f"network-{network_id}"
        else:
            _name = f"{prefix}{network_id}"
        _filters = [{"Name": "tag:Name", "Values": [_name]}]
        # Get all available peering connections
        _resp = self._cli.describe_vpcs(Filters=_filters)
        _vpcs = _resp[VPCS_LABEL]
        # Validate connection count
        if len(_vpcs) < 0:
            self._log.error(
                f"No VPC connection found for 'network-{network_id}'")
            return None
        elif len(_vpcs) > 1:
            self._log.warning("Found multiple VPC for "
                              f"'network-{network_id}'")
        return _vpcs[0]

    def get_vpc_peering_connection(self, _id):
        _filters = [{"Name": "vpc-peering-connection-id", "Values": [_id]}]
        _resp = self._cli.describe_vpc_peering_connections(Filters=_filters)
        _vpcs = _resp[VPCS_PEERING_LABEL]
        # Validate connection count
        if len(_vpcs) < 0:
            self._log.error(f"No VPC connection found for '{_id}'")
            return None
        elif len(_vpcs) > 1:
            self._log.warning("Found multiple VPC Peerings for "
                              f"'{_id}'")
        return _vpcs[0]

    def find_vpc_peering_connection(self, state, params):
        """
        Identifies peering connections by comparing
        AcceptorVpcInfo and RequestorVpcInfo
        """
        def _filter_item(key, value):
            return {"Name": key, "Values": [value]}

        # Create a filter to search for the peering connection
        _filters = [
            _filter_item("accepter-vpc-info.owner-id", params.peer_owner_id),
            _filter_item("accepter-vpc-info.vpc-id", params.peer_vpc_id),
            _filter_item("requester-vpc-info.owner-id", params.rp_owner_id),
            _filter_item("requester-vpc-info.vpc-id", params.rp_vpc_id),
            _filter_item("status-code", state),
            _filter_item("requester-vpc-info.cidr-block", params.network_cidr)
        ]
        # Get all available peering connections
        _resp = self._cli.describe_vpc_peering_connections(Filters=_filters)
        # Extract IDs
        _ids = [p[VPC_PEERING_ID_LABEL] for p in _resp[VPCS_PEERING_LABEL]]
        # Validate
        if len(_ids) > 1:
            self._log.warning("Found multiple peering "
                              f"connections (cleaning?): {_ids}")
        elif len(_ids) < 1:
            self._log.error(
                f"No peering connections found for given info: {_filters}")
            return None
        # At this point there surely is at least one item in the list
        self._log.info(f"Using peering connection with id: '{_ids[0]}'")
        return _ids[0]

    def accept_vpc_peering(self, peering_id, dry_run=False):
        """
        Accepts peering
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2/client/accept_vpc_peering_connection.html#
        """
        _r = self._cli.accept_vpc_peering_connection(
            DryRun=dry_run, VpcPeeringConnectionId=peering_id)
        if _r[RESPONCE_META_LABEL]["HTTPStatusCode"] != 200:
            self._log.warning("Unexpected return code while accepting peering"
                              f" connection with id '{peering_id}'")
        return _r[VPC_PEERING_LABEL] if VPC_PEERING_LABEL in _r else None

    def get_vpc_peering_status(self, vpc_peering_id):
        """
        Gets VPC Peering connection status by its id
        """
        _filters = [{
            "Name": "vpc-peering-connection-id",
            "Values": [vpc_peering_id]
        }]
        _r = self._cli.describe_vpc_peering_connections(Filters=_filters)
        # There can be only one such object, no need to validate
        return _r[VPCS_PEERING_LABEL][0]["Status"]["Code"]

    def get_route_tables_for_cluster(self, network_id):
        _filters = [{
            "Name": "tag:purpose",
            "Values": ["private"]
        }, {
            "Name": "tag:Name",
            "Values": [f"network-{network_id}"]
        }]
        _r = self.get_route_tables(filters=_filters)
        # TODO: Handle errors
        return _r

    def get_route_tables_for_vpc(self, vpc_id):
        _r = self.get_route_tables(filters=[{
            "Name": "vpc-id",
            "Values": [vpc_id]
        }])
        # TODO: Handle errors
        return _r

    def get_route_tables(self, ids=[], filters=[]):
        return self._cli.describe_route_tables(Filters=filters,
                                               RouteTableIds=ids)

    def create_route(self, rtb_id, destCidrBlock, vpc_peering_id):
        try:
            _r = self._cli.create_route(RouteTableId=rtb_id,
                                        DestinationCidrBlock=destCidrBlock,
                                        VpcPeeringConnectionId=vpc_peering_id)
        except ClientError as e:
            # Check if this is RouteAlreadyExists
            if 'RouteAlreadyExists' in e.response['Error']['Code']:
                # get existing route
                self._log.warning(f"Route already exists in table '{rtb_id}':"
                                  f" '{destCidrBlock}'/'{vpc_peering_id}'")
                return None
            else:
                raise RuntimeError(e)
        if _r and not _r['Return']:
            self._log.warning(f"Failed to create route in '{rtb_id}' "
                              f"for '{destCidrBlock}'")
        return _r

    def get_single_zone(self, region):
        """
        Get list of available zones based on region

        Sample return value
        {
            "AvailabilityZones": [
                {
                    "State": "available",
                    "OptInStatus": "opt-in-not-required",
                    "Messages": [],
                    "RegionName": "us-west-2",
                    "ZoneName": "us-west-2a",
                    "ZoneId": "usw2-az2",
                    "GroupName": "us-west-2",
                    "NetworkBorderGroup": "us-west-2",
                    "ZoneType": "availability-zone"
                },
                ...
            ]
        }
        """
        # Setup filter for current region just in case
        # Althrough, by default, only zones from current region will be listed
        _filters = [{"Name": "region-name", "Values": [region]}]
        # Call EC2 to get the list
        _r = self._cli.describe_availability_zones(Filters=_filters)
        return _r[AZS_LABEL][0][AZ_ID_LABEL]

    def get_instance_meta(self, target='localhost'):
        """
        Query meta from local node.
        Source: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-categories.html
        """
        _prefix = "http://"
        _suffix = "/latest/meta-data/"
        _target = "169.254.169.254" if target == 'localhost' else target
        uri = f"{_prefix}{_target}{_suffix}"
        # Get meta
        _meta = query_instance_meta(uri, headers=None)
        # swith macs for device ids
        # filter out keys for network interfaces
        # _old_keys = filter(lambda x: x.startswith('interfaces-macs'), _meta)
        # iterate keys and build new dict
        _new_meta = {}
        _head = "network-interfaces-macs-"
        for k, v in _meta.items():
            if not k.startswith(_head):
                _new_meta[k] = v
            else:
                # current mac
                _tail = k.replace(_head, "")
                _mac = _tail[:_tail.index('-')] if '-' in _tail else ""
                _tail = _tail[_tail.index('-'):] if '-' in _tail else _tail
                # if this is not interface key just copy
                # get id and create new key
                _id = _meta[_head + _mac + "-device-number"]
                _new_meta[_head + _id + _tail] = v
        return _new_meta
