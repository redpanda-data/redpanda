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
    oauth_url: str
    oauth_client_id: str
    oauth_client_secret: str
    oauth_audience: str
    api_url: str
    teleport_auth_server: str
    teleport_bot_token: str
    id: str
    delete_cluster: str

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
    region_id: str = None
    peer_vpc_id: str = None
    peer_owner_id: str = None
    rp_vpc_id: str = None
    rp_owner_id: str = None

    # Can't use mutables in defaults of dataclass
    # https://docs.python.org/3/library/dataclasses.html#dataclasses.field
    zones: list[str] = field(default_factory=list)


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

        self.provider_cli = make_provider_client(
            self.config.provider, logger, provider_config=provider_config)
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

        return self.config.id

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

    def _get_install_pack_ver(self):
        """Get the latest certified install pack version.

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
            if r['name'] == self.config.region:
                return r['id']
        return None

    def _get_product_id(self, config_profile_name):
        """Get the product id for the first matching config profile name using filter parameters.

        :param config_profile_name: config profile name, e.g. 'tier-1-aws'
        :param provider: cloud provider filter, e.g. 'AWS'
        :param cluster_type: cluster type filter, e.g. 'FMC'
        :param region: region name filter, e.g. 'us-west-2'
        :param install_pack_ver: install pack version filter, e.g. '23.2.20230707135118'
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
        return None

    def _create_cluster_payload(self):
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
                    "cidr": "10.1.0.0/16",
                    "deploymentType": self.config.type,
                    "installPackVersion": self.current.install_pack_ver,
                    "provider": self.config.provider,
                    "regionId": self.current.region_id,
                }
            },
        }

    def create(self,
               config_profile_name: str = 'tier-1-aws',
               superuser: Optional[SaslCredentials] = None) -> str:
        """Create a cloud cluster and a new namespace; block until cluster is finished creating.

        :param config_profile_name: config profile name, default 'tier-1-aws'
        :return: clusterId, e.g. 'cimuhgmdcaa1uc1jtabc'
        """

        if self.config.id != '':
            self._logger.warn('will not create cluster; already have '
                              f'cluster_id {self.config.id}')
            return self.config.id

        # In order not to have long list of arguments in each internal
        # functions, use self.current as a data store
        # Each of the _*_payload functions and _get_* will use it as a source
        # of data to create proper body for REST request
        self.current.namespace_uuid = self._create_namespace()
        # name rp-ducktape-cluster-3b36f516
        self.current.name = f'rp-ducktape-cluster-{self._unique_id}'
        self.current.install_pack_ver = self._get_install_pack_ver()
        self.current.region_id = self._get_region_id()
        self.current.zones = ['usw2-az1']
        self.current.product_id = self._get_product_id(config_profile_name)
        if not self.isPublicNetwork:
            self.current.connection_type = 'private'

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

    def _get_ducktape_meta(self, url):
        resp = requests.get(url)
        if not resp:
            self._logger.error("failed to get metadata from Ducktape node: "
                               f"'{url}'")
        return resp.text

    def create_vpc_peering(self):
        """
        Create VPC peering for deployed cloud with private network

        Steps:
        1. Get networkID of deployed cloud
        2. Confirm by calling cloudv2 api
        3. Get Deployed Redpanda cluster's VCP
        4. Get VPC of the ducktape client instance
        5a. Create initial peering
            vpc-id <Requester, Redpanda Cluster>
            peer-vpc-id <Accepter, Ducktape client>
            Create routes from Readpanda to Ducktape (10.10.xxx -> 172...)
            Create routes to Redpanda from Ducktape (172... -> 10.10.xxx)
        5b. Use CloudV2 API to create peering

        6. Accept VPC Peering on AWS side

        """

        # Network id
        network_id = self.current.network_id

        # get CIDR from network
        # _cidr =

        # VPC for the client can be found in metadata
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-categories.html
        # curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/$(ip -br link show eth0 | awk '{print$3}')/owner-id
        # curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/$(ip -br link show eth0 | awk '{print$3}')/vpc-id

        _ducktape_client_ip = "0.0.0.0"
        _baseurl = "http://169.254.169.254/latest/meta-data/network/interfaces/macs/"
        # Prepare metadata
        self.current.peer_vpc_id = self._get_ducktape_meta(
            f"{_baseurl}{_ducktape_client_ip}/vpc-id")
        self.current.peer_owner_id = self._get_ducktape_meta(
            f"{_baseurl}{_ducktape_client_ip}/owner-id")

        # Use Cloud API to create peering
        # Alternatively, Use manual AWS commands to do the same
        _body = self._create_network_peering_payload()
        self._logger.debug(f"body: '{_body}'")
        resp = self.cloudv2._http_post(
            f'/api/v1/networks/{network_id}/network-peerings', json=_body)

        # TODO accept the AWS VPC peering request
        # TODO create route between vpc and peering connection

        return
