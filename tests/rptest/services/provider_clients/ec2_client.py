import boto3
import json
import boto3.exceptions
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
        self._ec2 = boto3.resource('ec2', region_name=region)
        self._s3cli = self._make_s3_client()
        self._s3res = self._make_s3_resource()
        self._sts = self._make_sts_client()
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

    def _make_sts_client(self):
        return boto3.client('sts',
                            aws_access_key_id=self._access_key,
                            aws_secret_access_key=self._secret_key)

    def _make_s3_client(self):
        cfg = Config(region_name=self._region,
                     retries={
                         'max_attempts': 10,
                         'mode': 'adaptive'
                     })
        return boto3.client('s3',
                            config=cfg,
                            aws_access_key_id=self._access_key,
                            aws_secret_access_key=self._secret_key,
                            endpoint_url=self._endpoint,
                            use_ssl=not self._disable_ssl)

    def _make_s3_resource(self):
        cfg = Config(region_name=self._region,
                     retries={
                         'max_attempts': 10,
                         'mode': 'adaptive'
                     })
        return boto3.resource('s3',
                              config=cfg,
                              aws_access_key_id=self._access_key,
                              aws_secret_access_key=self._secret_key,
                              endpoint_url=self._endpoint,
                              use_ssl=not self._disable_ssl)

    def get_caller_identity(self):
        return self._sts.get_caller_identity()

    def _get_tag_for_resource(self, resource, key):
        for tag in resource.tags:
            if tag['Key'] == key:
                return tag['Value']
        return None

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

    def get_vpc_by_id(self, vpc_id):
        """Get aws VPC by its id
        """
        _resp = self._cli.describe_vpcs(VpcIds=[vpc_id])
        return _resp['Vpcs']

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
        if len(_vpcs) < 1:
            self._log.error(
                f"No VPC connection found for 'network-{network_id}'")
            return None
        elif len(_vpcs) > 1:
            self._log.warning("Found multiple VPC for "
                              f"'network-{network_id}'")
        return _vpcs[0]

    def list_network_interfaces(self, tag_key=None):
        _filters = []
        if tag_key is not None:
            _filters.append({"Name": "tag-key", "Values": [tag_key]})
        # List IFs
        _resp = self._cli.describe_network_interfaces(Filters=_filters)
        return _resp['NetworkInterfaces']

    def list_nat_gateways(self, tag_key=None):
        _filters = []
        if tag_key is not None:
            _filters.append({"Name": "tag-key", "Values": [tag_key]})
        # List NATs
        _resp = self._cli.describe_nat_gateways(Filters=_filters)
        return _resp['NatGateways']

    def list_eips_by_network_name(self, network_name):
        _filters = [{"Name": "tag:Name", "Values": [network_name]}]
        _resp = self._cli.describe_addresses(Filters=_filters)
        return _resp['Addresses']

    def get_nat_gateway(self, id):
        r = self._cli.describe_nat_gateways(NatGatewayIds=[id])
        # "There can be only one"
        return r['NatGateways'][0]

    def list_vpcs(self, tag_key=None):
        _filters = []
        if tag_key is not None:
            _filters.append({"Name": "tag-key", "Values": [tag_key]})
        # List VPCs
        _resp = self._cli.describe_vpcs(Filters=_filters)
        return _resp['Vpcs']

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

    def list_buckets(self, raw=False, mask=None):
        """
            List buckets with optional simple filtering
              raw: As it is returned by boto3.s3cli.list_buckets
              mask: string mask for name filtering: 'mask' in 'bucket_name'
        """
        if raw:
            # Just return RAW boto3 answer
            return self._s3cli.list_buckets()
        elif not mask:
            # Extract bucket list from boto3 return
            return self._s3cli.list_buckets()['Buckets']
        else:
            # Filter by mask
            buckets = []
            for bucket in self._s3cli.list_buckets()['Buckets']:
                if mask in bucket['Name']:
                    buckets.append(bucket)
            return buckets

    def query_bucket_lastmodified_date(self, name):
        """Function returns last modified date 
           based on first object key in the bucket
        """
        objects = self._s3cli.list_objects_v2(Bucket=name, MaxKeys=1)
        if objects['KeyCount'] == 0:
            return None
        else:
            return objects['Contents'][0]['LastModified']

    def list_bucket_objects(self, name, max_objects=3000):
        # Numner of objects collected
        objects_collected = 0
        # This will never be more than 1000
        # See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
        objects_per_request = 1000
        # Object list
        objects = []
        # Token to continue
        continuation_token = None
        while objects_collected < max_objects:
            # Calculate if this will be the last portion
            if objects_collected + objects_per_request > max_objects:
                # Get the remainder object count
                objects_per_request = max_objects - objects_collected
            # Prepare params
            kwargs = {"Bucket": name, "MaxKeys": objects_per_request}
            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token
            # get next portion
            new_objects = self._s3cli.list_objects_v2(**kwargs)
            # Get number of objects in response
            objects_collected += new_objects['KeyCount']
            # Get token to continue
            if 'NextContinuationToken' in new_objects:
                continuation_token = new_objects['NextContinuationToken']
            else:
                continuation_token = None
            # If collected, get object keys
            if objects_collected > 0:
                objects += new_objects['Contents']
            # If no token to continue, exit
            if not continuation_token:
                break

        return objects

    def delete_bucket(self, name):
        r = self._s3cli.delete_bucket(Bucket=name)
        return r

    def cleanup_bucket(self, name):
        """
            Deletes all objects in a bucket
        """
        # Shorthand to delete resources
        bucket = self._s3res.Bucket(name)
        _r = bucket.objects.delete()
        return _r

    def set_bucket_lifecycle_configuration(self, bucket_name, prefix=None):
        # Cleanup all keys in a bucket or by prefix
        lifecycle_config = {
            "Rules": [{
                "ID": "One Day",
                "Prefix": "",
                "Status": "Enabled",
                "Expiration": {
                    "Days": 1
                }
            }]
        }
        # Set prefix if any
        if prefix:
            lifecycle_config['Rules'][0]['Prefix'] = prefix
        # Convert policy to str and log
        lifecycle_policy = json.dumps(lifecycle_config)
        self._log.debug(lifecycle_policy)
        # Set policy for bucket
        r = self._s3cli.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
        return r

    def block_bucket_from_vpc(self, bucket_name, vpc_id):

        bucket_policy = {
            'Version':
            '2012-10-17',
            'Statement': [{
                'Sid': 'DenyPutFromRPNetwork',
                'Effect': 'Deny',
                'Principal': '*',
                'Action': ['s3:PutObject'],
                'Resource': f'arn:aws:s3:::{bucket_name}/*',
                'Condition': {
                    'ForAllValues:StringEquals': {
                        'aws:Ec2InstanceSourceVpc': vpc_id
                    }
                }
            }]
        }

        bucket_policy = json.dumps(bucket_policy)

        self._log.debug(bucket_policy)
        self._s3cli.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)

    def unblock_bucket_from_vpc(self, bucket_name):
        self._s3cli.delete_bucket_policy(Bucket=bucket_name)

    def release_address(self, allocationId):
        r = self._cli.release_address(AllocationId=allocationId)
        return r

    def disassociate_address(self, associationId):
        r = self._cli.disassociate_address(AssociationId=associationId)
        return r

    def delete_nat(self, natId):
        r = self._cli.delete_nat_gateway(NatGatewayId=natId)
        return r

    def delete_eni_by_vpc_id(self, vpc_id):
        """Release any Public IPs and delete interface
        """
        vpc_resource = self._ec2.Vpc(vpc_id)
        enis = vpc_resource.network_interfaces.all()
        if enis:
            for eni in list(enis):
                try:
                    # Just brute force delete
                    # if there is dependencies, just fail
                    eni.delete()
                    self._log.debug(
                        f"\t{vpc_id}: removed interface '{eni.id}'")
                except boto3.exceptions.Boto3Error as e:
                    self._log.error(f"\t{vpc_id}: Failed to Detach/Delete "
                                    f"network interface of '{eni.id}': {e}")
        return

    def delete_igw_by_vpc_id(self, vpc_id):
        """Detach and delete the internet-gateway
        """
        vpc_resource = self._ec2.Vpc(vpc_id)
        internet_gateways = vpc_resource.internet_gateways.all()
        if internet_gateways:
            for gateway in list(internet_gateways):
                try:
                    network_name = self._get_tag_for_resource(
                        vpc_resource, 'Name')
                    eips = self.list_eips_by_network_name(network_name)
                    if eips:
                        for eip in eips:
                            self._log.info(f"Releasing eIP: {eip}")
                            self._cli.release_address()

                    gateway.detach_from_vpc(VpcId=vpc_id)
                    self._log.debug(f"\t{vpc_id}: Detached internet gateway "
                                    f"'{gateway.id}'")
                    gateway.delete()
                    self._log.debug(f"\t{vpc_id}: Deleted internet gateway "
                                    f"'{gateway.id}'")
                except boto3.exceptions.Boto3Error as e:
                    self._log.error(f"\t{vpc_id}: Failed to Detach/Delete "
                                    f"internet gateway of '{gateway.id}': {e}")

        return

    def delete_subnet_by_vpc_id(self, vpc_id):
        """Delete the subnets using vpc_id
        """
        vpc_resource = self._ec2.Vpc(vpc_id)
        subnets = vpc_resource.subnets.all()
        # Delete all subnets
        if subnets:
            try:
                for subnet in subnets:
                    subnet.delete()
                    self._log.debug(
                        f"\t{vpc_id}: removed subnet '{subnet.id}'")
            except boto3.exceptions.Boto3Error as e:
                self._log.error(f"\t{vpc_id}: Failed to remove subnet "
                                f"'{subnet.id}': {e}")
        return

    def delete_route_table_by_vpc_id(self, vpc_id):
        """Delete route-tables using vpc_id
        """
        vpc_resource = self._ec2.Vpc(vpc_id)
        route_tables = vpc_resource.route_tables.all()

        if route_tables:
            try:
                for rtable in route_tables:
                    for route in rtable.routes:
                        if route.origin == 'CreateRoute':
                            route.delete()
                            # self._cli.delete_route(RouteTableId=rtable['RouteTableId'], DestinationCidrBlock=route['DestinationCidrBlock'])
                    for association in rtable.associations:
                        if not association.main:
                            association.delete()
                            # self._cli.disassociate_route_table(AssociationId=association['RouteTableAssociationId'])
                    if len(rtable.associations) == 0:
                        table = self._ec2.RouteTable(rtable.id)
                        table.delete()
                        self._log.debug(
                            f"\t{vpc_id}: removed routing table '{rtable.id}'")
            except boto3.exceptions.Boto3Error as e:
                self._log.error(f"\t{vpc_id}: Failed to remove routing table "
                                f"'{rtable.id}': {e}")
        return

    def delete_acl_by_vpc_id(self, vpc_id):
        """Delete the network-access-lists using vpc_id
        """

        vpc_resource = self._ec2.Vpc(vpc_id)
        acls = vpc_resource.network_acls.all()

        if acls:
            try:
                for acl in acls:
                    if acl.is_default:
                        self._log.info(
                            f"\t{vpc_id}: skipped: '{acl.id}' is the default NACL"
                        )
                        continue
                    acl.delete()
                    self._log.debug(f"\t{vpc_id}: removed ACL '{acl.id}'")
            except boto3.exceptions.Boto3Error as e:
                self._log.error(f"\t{vpc_id}: Failed to remove network ACL "
                                f"'{acl.id}': {e}")
        return

    def delete_secgroup_by_vpc_id(self, vpc_id):
        """Delete any security-groups using vpc_id
        """
        vpc_resource = self._ec2.Vpc(vpc_id)
        sgroups = vpc_resource.security_groups.all()
        if sgroups:
            try:
                for sg in sgroups:
                    if sg.group_name == 'default':
                        self._log.info(
                            f"Skipped: '{sg.id}' is the default security group"
                        )
                        continue
                    sg.delete()
                    self._log.debug(
                        f"\t{vpc_id}: removed security group '{sg.id}'")
            except boto3.exceptions.Boto3Error as e:
                self._log.error(f"\t{vpc_id}: Failed to remove security group "
                                f"'{sg.id}': {e}")
        return

    def delete_vpc(self, vpc_id, clean_dependencies=False):
        """ Delete VPC using its id
        """
        def safe_clean(f, *args):
            try:
                f(*args)
            except Exception as e:
                return False

        vpc_resource = self._ec2.Vpc(vpc_id)
        if clean_dependencies:
            try:
                # There is plenty of resources to cleanup

                # - Delete network interfaces
                # Enable manually. Normally these should be empty
                # if resources up the chain was properly cleaned out.
                # I.e. EKS clusters, ELBs
                # self.delete_eni_by_vpc_id(vpc_id)

                # - Delete the internet-gateway
                self.delete_igw_by_vpc_id(vpc_id)
                # - Delete subnets
                self.delete_subnet_by_vpc_id(vpc_id)
                # - Delete route-tables
                self.delete_route_table_by_vpc_id(vpc_id)
                # - Delete network access-lists
                self.delete_acl_by_vpc_id(vpc_id)
                # - Delete security-groups
                self.delete_secgroup_by_vpc_id(vpc_id)

                # - Delete the VPC
                vpc_resource.delete(
                    # DryRun=True
                )
            except Exception as e:
                self._log.warning(
                    f"\t{vpc_id}: Remove dependencies and delete VPC manually: {e}"
                )
                return False
        return True
