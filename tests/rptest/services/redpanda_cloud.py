import collections
import json
import os
import requests
import uuid

from dataclasses import dataclass, field
from typing import Optional

from ducktape.utils.util import wait_until

from rptest.services.provider_clients import make_provider_client

SaslCredentials = collections.namedtuple("SaslCredentials",
                                         ["username", "password", "algorithm"])

CLOUD_TYPE_FMC = 'FMC'
CLOUD_TYPE_BYOC = 'BYOC'


class RpCloudApiClient(object):
    def __init__(self, config, log):
        self._config = config
        self._token = None
        self._logger = log

    def _get_token(self):
        """
        Returns access token to be used in subsequent api calls to cloud api.

        To save on repeated token generation, this function will cache it in a local variable.
        Assumes the token has an expiration that will last throughout the usage of this cluster.

        :return: access token as a string
        """

        if self._token is None:
            headers = {'Content-Type': "application/x-www-form-urlencoded"}
            data = {
                'grant_type': 'client_credentials',
                'client_id': f'{self._config.oauth_client_id}',
                'client_secret': f'{self._config.oauth_client_secret}',
                'audience': f'{self._config.oauth_audience}'
            }
            resp = requests.post(f'{self._config.oauth_url}',
                                 headers=headers,
                                 data=data)
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                self._logger.error(f'{e} {resp.text}')
                return None
            j = resp.json()
            self._token = j['access_token']
        return self._token

    def _http_get(self, endpoint='', **kwargs):
        token = self._get_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
        resp = requests.get(f'{self._config.api_url}{endpoint}',
                            headers=headers,
                            **kwargs)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            self._logger.error(f'{e} {resp.text}')
            return None
        return resp.json()

    def _http_post(self, base_url=None, endpoint='', **kwargs):
        token = self._get_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
        if base_url is None:
            base_url = self._config.api_url
        resp = requests.post(f'{base_url}{endpoint}',
                             headers=headers,
                             **kwargs)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            self._logger.error(f'{e} {resp.text}')
            return None
        return resp.json()

    def _http_delete(self, endpoint='', **kwargs):
        token = self._get_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
        resp = requests.delete(f'{self._config.api_url}{endpoint}',
                               headers=headers,
                               **kwargs)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            self._logger.error(f'{e} {resp.text}')
            return None
        return resp.json()


@dataclass(kw_only=True)
class CloudClusterConfig:
    """
    Configuration of Cloud cluster.
    Should be in sync with cloud_cluster subsection in
    vtools/qa/deploy/ansible/roles/ducktape-setup/templates/ducktape_globals.json.j2
    """
    oauth_url: str = ""
    oauth_client_id: str = ""
    oauth_client_secret: str = ""
    oauth_audience: str = ""
    api_url: str = ""
    teleport_auth_server: str = ""
    teleport_bot_token: str = ""
    id: str = ""  # empty string makes it easier to pass thru default value from duck.py
    delete_cluster: bool = True

    region: str = "us-west-2"
    provider: str = "AWS"
    type: str = "FMC"
    network: str = "public"


@dataclass
class LiveClusterParams:
    """
    Active Cluster params.
    Should not be used outside of the redpanda_cloud module
    """
    isAlive: bool = False
    connection_type: str = 'public'
    namespace_uuid: str = None
    name: str = None
    network_id: str = None
    network_cidr: str = None
    install_pack_ver: str = None
    product_id: str = None
    region: str = None
    region_id: str = None
    peer_vpc_id: str = None
    peer_owner_id: str = None
    rp_vpc_id: str = None
    rp_owner_id: str = None
    aws_vpc_peering_id: str = None

    # Can't use mutables in defaults of dataclass
    # https://docs.python.org/3/library/dataclasses.html#dataclasses.field
    zones: list[str] = field(default_factory=list)
    aws_vpc_peering: dict = field(default_factory=dict)

    @property
    def network_endpoint(self):
        return f'/api/v1/networks/{self.network_id}/network-peerings'


class CloudCluster():
    """
    Operations on a Redpanda Cloud cluster via the swagger API.

    Creates and deletes a cluster. Will also create a new namespace for
    that cluster.
    """

    CHECK_TIMEOUT_SEC = 3600
    CHECK_BACKOFF_SEC = 60.0

    def __init__(self,
                 logger,
                 cluster_config,
                 delete_namespace=False,
                 provider_config=None):
        """
        Initializes the object, but does not create clusters. Use
        `create` method to create a cluster.


        :param logger: logging object
        :param cluster_config: dict object loaded from
               context.globals["cloud_cluster"]
        :param delete_namespace: if False, will skip namespace deletion
               step after tests are run
        """

        self._logger = logger
        self._delete_namespace = delete_namespace
        # JSON is serialized directly to dataclass
        self.config = CloudClusterConfig(**cluster_config)
        # ensure that variables are in correct case
        self.config.type = self.config.type.upper()
        self.config.provider = self.config.provider.upper()
        self.config.network = self.config.network.lower()
        # Init API client
        self.cloudv2 = RpCloudApiClient(self.config, logger)

        # Create helper bool variable
        self.isPublicNetwork = self.config.network == 'public'

        # unique 8-char identifier to be used when creating names of things
        # for this cluster
        self._unique_id = str(uuid.uuid1())[:8]

        # init live cluster params
        self.current = LiveClusterParams()
        # Saving key in case of cloud has different region
        self.provider_key = provider_config['access_key']
        self.provider_secret = provider_config['secret_key']
        self.provider_cli = make_provider_client(self.config.provider, logger,
                                                 self.config.region,
                                                 self.provider_key,
                                                 self.provider_secret)
        # Currently we need provider client only for VCP in private networking
        # Raise exception is client in not implemented yet
        if self.provider_cli is None and self.config.network != 'public':
            self._logger.error(
                f"Current provider is not yet supports private networking ")
            raise RuntimeError("Private networking is not implemented "
                               f"for '{self.config.provider}'")

    @property
    def cluster_id(self):
        """
        The clusterId of the created cluster.
        """

        return self._cluster_id

    def _create_namespace(self):
        # format namespace name as 'rp-ducktape-ns-3b36f516
        name = f'rp-ducktape-ns-{self._unique_id}'
        self._logger.debug(f'creating namespace name {name}')
        body = {'name': name}
        r = self.cloudv2._http_post(endpoint='/api/v1/namespaces', json=body)
        self._logger.debug(f'created namespaceUuid {r["id"]}')
        # save namespace name
        self.config.namespace = name
        return r['id']

    def _cluster_ready(self):
        self._logger.debug('checking readiness of '
                           f'cluster {self.current.name}')
        params = {'namespaceUuid': self.current.namespace_uuid}
        clusters = self.cloudv2._http_get(endpoint='/api/v1/clusters',
                                          params=params)
        for c in clusters:
            if c['name'] == self.current.name:
                if c['state'] == 'ready':
                    return True
                elif c['state'] == 'unknown':
                    raise RuntimeError("Creation failed (state 'unknown') "
                                       f"for '{self.config.provider}'")
        return False

    def _get_cluster_id_and_network_id(self):
        """
        Get clusterId.
        Uses self.current data as a source of needed params:
        self.current.namespace_uuid: namespaceUuid the cluster is contained in
        self.current.name: name of the cluster

        :return: clusterId, networkId as a string or None if not found
        """

        params = {'namespaceUuid': self.current.namespace_uuid}
        clusters = self.cloudv2._http_get(endpoint='/api/v1/clusters',
                                          params=params)
        for c in clusters:
            if c['name'] == self.current.name:
                return (c['id'], c['spec']['networkId'])
        return None, None

    def _get_network(self):
        return self.cloudv2._http_get(
            endpoint=f"/api/v1/networks/{self.current.network_id}")

    def _get_latest_install_pack_ver(self):
        """Get latest certified install pack ver by searching list of avail.

        :return: version, e.g. '23.2.20230707135118'
        """

        versions = self.cloudv2._http_get(
            endpoint='/api/v1/clusters-resources/install-pack-versions')
        latest_version = ''
        for v in versions:
            if v['certified'] and v['version'] > latest_version:
                latest_version = v['version']
        if latest_version == '':
            return None
        return latest_version

    def _get_region_id(self):
        """Get the region id for a region.

        :param cluster_type: cluster type, e.g. 'FMC'
        :param provider: cloud provider, e.g. 'AWS'
        :param region: region name, e.g. 'us-west-2'
        :return: id, e.g. 'cckac9vvbr5ofm048jjg'
        """

        params = {'cluster_type': self.config.type}
        regions = self.cloudv2._http_get(
            endpoint='/api/v1/clusters-resources/regions', params=params)
        for r in regions[self.config.provider]:
            if r['name'] == self.current.region:
                return r['id']
        return None

    def _get_product_id(self, config_profile_name):
        """Get the product id for the first matching config
        profile name using filter parameters.
        Uses self.current as a source of params
            provider: cloud provider filter, e.g. 'AWS'
            cluster_type: cluster type filter, e.g. 'FMC'
            region: region name filter, e.g. 'us-west-2'
            install_pack_ver: install pack version filter,
               e.g. '23.2.20230707135118'

        :param config_profile_name: config profile name, e.g. 'tier-1-aws'
        :return: productId, e.g. 'chqrd4q37efgkmohsbdg'
        """

        params = {
            'cloud_provider': self.config.provider,
            'cluster_type': self.config.type,
            'region': self.config.region,
            'install_pack_version': self.current.install_pack_ver
        }
        products = self.cloudv2._http_get(
            endpoint='/api/v1/clusters-resources/products', params=params)
        for p in products:
            if p['redpandaConfigProfileName'] == config_profile_name:
                return p['id']
        self._logger.warning("CloudV2 API returned empty 'product_id' list "
                             f"for request: '{params}'")
        return None

    def _create_cluster_payload(self):
        _cidr = "10.1.0.0/16" if self.isPublicNetwork else self.config.network
        return {
            "cluster": {
                "name": self.current.name,
                "productId": self.current.product_id,
                "spec": {
                    "clusterType": self.config.type,
                    "connectors": {
                        "enabled": True
                    },
                    "installPackVersion": self.current.install_pack_ver,
                    "isMultiAz": False,
                    "networkId": "",
                    "provider": self.config.provider,
                    "region": self.config.region,
                    "zones": self.current.zones,
                }
            },
            "connectionType": self.current.connection_type,
            "namespaceUuid": self.current.namespace_uuid,
            "network": {
                "displayName":
                f"{self.current.connection_type}-network-{self.current.name}",
                "spec": {
                    "cidr": _cidr,
                    "deploymentType": self.config.type,
                    "installPackVersion": self.current.install_pack_ver,
                    "provider": self.config.provider,
                    "regionId": self.current.region_id,
                }
            },
        }

    def _get_cluster(self, _id):
        """
        Calls CloudV2 API to get cluster info
        """
        _endpoint = f"/api/v1/clusters/{_id}"
        return self.cloudv2._http_get(endpoint=_endpoint)

    def _update_live_cluster_info(self):
        """
        Update info from existing cluster (BYOC)
        """
        # get cluster data
        _c = self._get_cluster(self.config.id)
        # Fill in immediate configuration
        self.current.isAlive = True if _c['status'][
            'health'] == 'healthy' else False
        self.current.name = _c['name']
        self.current.namespace_uuid = _c['namespaceUuid']
        self.current.install_pack_ver = _c['spec']['installPackVersion']
        self.current.region = _c['spec']['region']
        self.current.region_id = self._get_region_id()
        self.current.zones = _c['spec']['zones']
        self.current.network_id = _c['spec']['networkId']
        self.current.network_cidr = _c['spec']['network']['networkCidr']
        if self.current.region != self.config.region:
            raise RuntimeError("BYOC Cluster is in different region: "
                               f"'{self.current.region}'. Multi-region "
                               "testing is not supported at this time.")

        return

    def create(self,
               config_profile_name: str = 'tier-1-aws',
               superuser: Optional[SaslCredentials] = None) -> str:
        """Create a cloud cluster and a new namespace; block until cluster is finished creating.

        :param config_profile_name: config profile name, default 'tier-1-aws'
        :return: clusterId, e.g. 'cimuhgmdcaa1uc1jtabc'
        """

        if not self.isPublicNetwork:
            self.current.connection_type = 'private'

        if self.config.id != '':
            # Cluster already exist
            # Just load needed info to create peering
            self._logger.warn('will not create cluster; already have '
                              f'cluster_id {self.config.id}')
            # Populate self.current from cluster info
            self._update_live_cluster_info()
            # Fill in additional info based on collected from cluster
            self.current.product_id = self._get_product_id(config_profile_name)
            # If network type is private, trigget VPC Peering
            if not self.isPublicNetwork:
                # Create VPC peering
                self.create_vpc_peering()
            return self.config.id

        # In order not to have long list of arguments in each internal
        # functions, use self.current as a data store
        # Each of the _*_payload functions and _get_* will use it as a source
        # of data to create proper body for REST request
        self.current.namespace_uuid = self._create_namespace()
        # name rp-ducktape-cluster-3b36f516
        self.current.name = f'rp-ducktape-cluster-{self._unique_id}'
        # Install pack handling
        if self.config.install_pack_ver == 'latest':
            self.config.install_pack_ver = self._get_latest_install_pack_ver()
        self.current.install_pack_ver = self.config.install_pack_ver
        # Multi-zone not supported, so get a single one from the list
        self.current.region = self.config.region
        self.current.region_id = self._get_region_id()
        self.current.zones = self.provider_cli.get_single_zone(
            self.current.region)
        # Call CloudV2 API to determine Product ID
        self.current.product_id = self._get_product_id(config_profile_name)
        if self.current.product_id is None:
            raise RuntimeError("ProductID failed to be determined for "
                               f"'{self.config.provider}', "
                               f"'{self.config.type}', "
                               f"'{self.config.install_pack_ver}', "
                               f"'{self.config.region}'")

        # Call Api to create cluster
        self._logger.info(f'creating cluster name {self.current.name}')
        # Prepare cluster payload block
        _body = self._create_cluster_payload()
        self._logger.debug(f'body: {json.dumps(_body)}')
        # Send API request
        r = self.cloudv2._http_post(
            endpoint='/api/v1/workflows/network-cluster', json=_body)

        self._logger.info(
            f'waiting for creation of cluster {self.current.name} '
            f'namespaceUuid {r["namespaceUuid"]}, checking every '
            f'{self.CHECK_BACKOFF_SEC} seconds')
        # Poll API and wait for the cluster creation
        wait_until(lambda: self._cluster_ready(),
                   timeout_sec=self.CHECK_TIMEOUT_SEC,
                   backoff_sec=self.CHECK_BACKOFF_SEC,
                   err_msg='Unable to deterimine readiness '
                   f'of cloud cluster {self.current.name}')
        self.config.id, self.current.network_id = \
            self._get_cluster_id_and_network_id()

        if superuser is not None:
            self._logger.debug(f'super username: {superuser.username}, '
                               f'algorithm: {superuser.algorithm}')
            self._create_user(superuser)
            self._create_acls(superuser.username)

        if not self.isPublicNetwork:
            self.create_vpc_peering()
        self.current.isAlive = True
        return self.config.id

    def delete(self):
        """
        Deletes a cloud cluster and the namespace it belongs to.
        """

        if self.config.id == '':
            self._logger.warn(f'cluster_id is empty, unable to delete cluster')
            return
        elif not self.current.delete_cluster:
            self._logger.warn(f'Cluster deletion skipped as configured')
            return

        resp = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.config.id}')
        namespace_uuid = resp['namespaceUuid']

        resp = self.cloudv2._http_delete(
            endpoint=f'/api/v1/clusters/{self.config.id}')
        self._logger.debug(f'resp: {json.dumps(resp)}')
        self.config.id = ''

        # skip namespace deletion to avoid error because cluster delete not complete yet
        if self._delete_namespace:
            resp = self.cloudv2._http_delete(
                endpoint=f'/api/v1/namespaces/{namespace_uuid}')
            self._logger.debug(f'resp: {json.dumps(resp)}')

    def _create_user(self, user: SaslCredentials):
        """Create SASL user
        """

        cluster = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.config.id}')
        base_url = cluster['status']['listeners']['redpandaConsole'][
            'default']['urls'][0]
        payload = {
            'mechanism': user.algorithm,
            'password': user.password,
            'username': user.username,
        }
        # use the console api url to create sasl users; uses the same auth token
        return self.cloudv2._http_post(base_url=base_url,
                                       endpoint='/api/users',
                                       json=payload)

    def _create_acls(self, username):
        """Create ACLs for user
        """

        cluster = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.config.id}')
        base_url = cluster['status']['listeners']['redpandaConsole'][
            'default']['urls'][0]
        for rt in ('Topic', 'Group', 'TransactionalID'):
            payload = {
                'host': '*',
                'operation': 'All',
                'permissionType': 'Allow',
                'principal': f'User:{username}',
                'resourceName': '*',
                'resourcePatternType': 'Literal',
                'resourceType': rt,
            }
            self.cloudv2._http_post(base_url=base_url,
                                    endpoint='/api/acls',
                                    json=payload)

        payload = {
            'host': '*',
            'operation': 'All',
            'permissionType': 'Allow',
            'principal': f'User:{username}',
            'resourceName': 'kafka-cluster',
            'resourcePatternType': 'Literal',
            'resourceType': 'Cluster',
        }
        self.cloudv2._http_post(base_url=base_url,
                                endpoint='/api/acls',
                                json=payload)

    def get_broker_address(self):
        cluster = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.config.id}')
        return cluster['status']['listeners']['kafka']['default']['urls'][0]

    def get_install_pack_version(self):
        cluster = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.config.id}')
        return cluster['status']['installPackVersion']

    def _create_network_peering_payload(self):
        return {
            "networkPeering": {
                "displayName": f'peer-{self.current.name}',
                "spec": {
                    "provider": "AWS",
                    "cloudProvider": {
                        "aws": {
                            "peerOwnerId": self.current.peer_owner_id,
                            "peerVpcId": self.current.peer_vpc_id
                        }
                    }
                }
            },
            "namespaceUuid": self.current.namespace_uuid
        }

    def _get_ducktape_client_mac_uri(self):
        """
        Get MAC address from current interface.
        Function assumes that there is a single interface on ducktape node
        """
        _url = "http://169.254.169.254/latest/meta-data/network/interfaces/macs"
        resp = requests.get(_url)
        if not resp.ok:
            self._logger.error("failed to get MAC for Ducktape node: "
                               f"'{_url}'")
            return None
        else:
            # Example: '0a:97:f4:40:30:93/'
            return f"{_url}/{resp.text}"

    def _get_ducktape_meta(self):
        """
        Query meta from local node.
        Source: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-categories.html
        Available for AWS instance:
            device-number
            interface-id
            ipv4-associations/
            local-hostname
            local-ipv4s
            mac
            owner-id
            public-hostname
            public-ipv4s
            security-group-ids
            security-groups
            subnet-id
            subnet-ipv4-cidr-block
            vpc-id
            vpc-ipv4-cidr-block
            vpc-ipv4-cidr-blocks
        """
        _url = self._get_ducktape_client_mac_uri()
        resp = requests.get(_url)
        if not resp.ok:
            self._logger.error(
                "failed to get metadata list from Ducktape node: "
                f"'{_url}'")
            return None
        else:
            # Assume that since first request passed,
            # There is no need to validate each one.
            # build dict with meta
            _keys = resp.text.split()
            _values = [requests.get(f"{_url}/{key}").text for key in _keys]
            _meta = {}
            for item in zip(_keys, _values):
                _meta[item[0]] = item[1]
            return _meta

    def _prepare_fmc_network_vpc_info(self):
        """
        Calls CloudV2 API to get cidr_block, vpc_id
        and owner_id of created cluster
        """
        # For FMC network is in CloudV2 account
        # so info is coming from CloudV2 API
        _net = self._get_network()
        _info = _net['status']['created']['providerNetworkDetails'][
            'cloudProvider'][self.config.provider.lower()]
        self.current.rp_vpc_id = _info['vpcId']
        self.current.rp_owner_id = _info['ownerId']
        self.current.network_cidr = _info['cidrBlock']

    def _prepare_byoc_network_vpc_info(self):
        # Network is handled by provider
        _net = self.provider_cli.get_vpc_by_network_id(self.current.network_id)
        self.current.rp_vpc_id = _net['VpcId']
        self.current.rp_owner_id = _net['OwnerId']
        self.current.network_cidr = _net['CidrBlock']

        return

    def _get_vpc_peering_connection(self, endpoint):
        """
        Get VPC peering connection from CloudV2
        """
        _endpoint = f"{endpoint}/{self.vpc_peering['id']}"
        return self.cloudv2._http_get(endpoint=_endpoint)

    def _check_peering_status_cluster(self, endpoint, state):
        """
        Check VPC peering Status on CloudV2 side
        """
        _peering = self._get_vpc_peering_connection(endpoint)
        if _peering['state'] == state:
            self.vpc_peering = _peering
            return True
        else:
            return False

    def _wait_peering_status_cluster(self, state):
        # Wait for 'ready' on CloudV2 side
        wait_until(lambda: self._check_peering_status_cluster(
            self.current.network_endpoint, state),
                   timeout_sec=self.CHECK_TIMEOUT_SEC,
                   backoff_sec=self.CHECK_BACKOFF_SEC,
                   err_msg=f'Timeout waiting for {state} status '
                   f'of peering {self.current.name}')

    def _check_peering_status_provider(self, _vpc_peering_id, state):
        """
        Check VPC Peering connection status on AWS side
        """
        _state = self.provider_cli.get_vpc_peering_status(_vpc_peering_id)
        if _state == state:
            return True
        else:
            return False

    def _wait_peering_status_provider(self, state):
        # Wait for 'active' on AWS side
        wait_until(lambda: self._check_peering_status_provider(
            self.current.aws_vpc_peering_id, state),
                   timeout_sec=self.CHECK_TIMEOUT_SEC,
                   backoff_sec=self.CHECK_BACKOFF_SEC,
                   err_msg=f'Timeout waiting for {state} status '
                   f'of peering {self.current.name}')

        return

    def _create_routes_to_ducktape(self):
        # Create routes from CloudV2 to Ducktape (10.10.xxx -> 172...)
        # aws ec2 describe-route-tables --filter "Name=tag:Name,Values=network-cjah1ecce8edpeoj0li0" "Name=tag:purpose,Values=private" | jq -r '.RouteTables[].RouteTableId' | while read -r route_table_id; do aws ec2 create-route --route-table-id $route_table_id --destination-cidr-block 172.31.0.0/16 --vpc-peering-connection-id pcx-0581b037d9e93593e; done;
        # FMC: handled by CloudV2 and ID count get_rtb results in zero
        # BYOC: Should create routes
        _rtbs = self.provider_cli.get_route_table_ids_for_cluster(
            self.current.network_id)
        for _rtb_id in _rtbs:
            self.provider_cli.create_route(
                _rtb_id,
                self.current.aws_vpc_peering['AccepterVpcInfo']['CidrBlock'],
                self.current.aws_vpc_peering_id)

        return

    def _create_routes_to_cluster(self):
        # Create routes from Ducktape to CloudV2
        # aws ec2 --region us-west-2 create-route --route-table-id rtb-02e89e44cb4da000d --destination-cidr-block 10.10.0.0/16 --vpc-peering-connection-id pcx-0581b037d9e93593e
        _rtbs = self.provider_cli.get_route_table_ids_for_vpc(
            self.current.peer_vpc_id)
        for _rtb_id in _rtbs:
            self.provider_cli.create_route(
                _rtb_id,
                self.current.aws_vpc_peering['RequesterVpcInfo']['CidrBlock'],
                self.current.aws_vpc_peering_id)

        return

    def _create_vpc_peering_fmc(self):
        """
        FMC VPC Peering
        1. Prepare vpc info using CloudV2
        2. Create peering in CloudV2
        3. Wait for Pending Acceptance usign CloudV2
        4. Accept it
        5. Create routes
        6. Ensure Ready/Active state on both sides
        """
        # 1.
        self._prepare_fmc_network_vpc_info()

        # 2. Use Cloud API to create peering
        # Alternatively, Use manual AWS commands to do the same
        _body = self._create_network_peering_payload()
        self._logger.debug(f"body: '{_body}'")

        # Create peering
        resp = self.cloudv2._http_post(endpoint=self.current.network_endpoint,
                                       json=_body)
        self._logger.debug(f"Created VPC peering: '{resp}'")
        self.vpc_peering = resp

        # 3.
        # Wait for "pending acceptance"
        self._wait_peering_status_cluster("pending acceptance")

        # Find id of the correct peering VPC
        self.current.aws_vpc_peering_id = self.provider_cli.find_vpc_peering_connection(
            "pending-acceptance", self.current)

        # 4. Accept it on AWS
        self.current.aws_vpc_peering = self.provider_cli.accept_vpc_peering(
            self.current.aws_vpc_peering_id)

        # 5.
        self._create_routes_to_ducktape()
        self._create_routes_to_cluster()

        # 6.
        self._wait_peering_status_provider("active")
        self._wait_peering_status_cluster("ready")

        return

    def _create_vpc_peering_byoc(self):
        """
        BYOC VPC Peering.
        This uses own EC2 client if region is different

        1. Prepare VPC info using boto3/vpc
        2. Create peering in AWS
        3. Wait for pending acceptance using AWS
        4. Accept it
        5. Create routes
        6. Ensure Active on AWS
        """
        # 1.
        self._prepare_byoc_network_vpc_info()

        # 2.
        # AWS will not create duplicate peering connection
        # Will just return existing one
        self.current.aws_vpc_peering_id = self.provider_cli.create_vpc_peering(
            self.current)

        # 3.
        # Check if this is active already
        _is_active = self._check_peering_status_provider(
            self.current.aws_vpc_peering_id, "active")
        if not _is_active:
            self._wait_peering_status_provider("pending-acceptance")
            # 4.
            # If this is freshly created, accept it
            self.current.aws_vpc_peering = self.provider_cli.accept_vpc_peering(
                self.current.aws_vpc_peering_id)
        else:
            self.current.aws_vpc_peering = \
                self.provider_cli.get_vpc_peering_connection(
                    self.current.aws_vpc_peering_id)

        # 5.
        self._create_routes_to_ducktape()
        self._create_routes_to_cluster()

        # 6.
        # No need to wait if its active already
        if not _is_active:
            self._wait_peering_status_provider('active')

        return

    def create_vpc_peering(self):
        """
        Create VPC peering for deployed cloud with private network

        Steps:
        1. Get VPC of the ducktape client instance
        2. Detect cloud type and run corresponding workflow

        """

        # 1.
        self._ducktape_meta = self._get_ducktape_meta()
        self.current.peer_vpc_id = self._ducktape_meta['vpc-id']
        self.current.peer_owner_id = self._ducktape_meta['owner-id']
        # 2.
        # Prepare vpc_id, owner_id and cidrBlock for cloud cluster
        if self.config.type == CLOUD_TYPE_FMC:
            # Workflow for FMC cloud
            self._create_vpc_peering_fmc()
        elif self.config.type == CLOUD_TYPE_BYOC:
            # Workflow for BYOC
            self._create_vpc_peering_byoc()
        else:
            self._logger.error(
                f"Cloud type '{self.config.type}' not supported")

        return
