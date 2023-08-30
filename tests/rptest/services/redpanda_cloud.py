import collections
import json
import os
import requests
import uuid
import yaml
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from ducktape.utils.util import wait_until
from rptest.services.provider_clients import make_provider_client

rp_profiles_path = os.path.join(os.path.dirname(__file__),
                                "rp_config_profiles")
tiers_config_filename = os.path.join(rp_profiles_path,
                                     "redpanda.cloud-tiers-config.yml")


def load_tier_profiles():
    with open(os.path.join(os.path.dirname(__file__), tiers_config_filename),
              "r") as f:
        # TODO: validate input
        _profiles = yaml.safe_load(f)['config_profiles']
    return _profiles


SaslCredentials = collections.namedtuple("SaslCredentials",
                                         ["username", "password", "algorithm"])

CLOUD_TYPE_FMC = 'FMC'
CLOUD_TYPE_BYOC = 'BYOC'
PROVIDER_AWS = 'AWS'
PROVIDER_GCP = 'GCP'

TIER_DEFAULTS = {PROVIDER_AWS: "tier-1-aws", PROVIDER_GCP: "tier-1-gcp"}


class CloudTierName(Enum):
    AWS_1 = 'tier-1-aws'
    AWS_2 = 'tier-2-aws'
    AWS_3 = 'tier-3-aws'
    AWS_4 = 'tier-4-aws'
    AWS_5 = 'tier-5-aws'
    GCP_1 = 'tier-1-gcp'
    GCP_2 = 'tier-2-gcp'
    GCP_3 = 'tier-3-gcp'
    GCP_4 = 'tier-4-gcp'
    GCP_5 = 'tier-5-gcp'

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


class AdvertisedTierConfig:
    def __init__(self, ingress_rate: float, egress_rate: float,
                 num_brokers: int, segment_size: int, cloud_cache_size: int,
                 partitions_min: int, partitions_max: int,
                 connections_limit: Optional[int],
                 memory_per_broker: int) -> None:
        self.ingress_rate = int(ingress_rate)
        self.egress_rate = int(egress_rate)
        self.num_brokers = num_brokers
        self.segment_size = segment_size
        self.cloud_cache_size = cloud_cache_size
        self.partitions_min = partitions_min
        self.partitions_max = partitions_max
        self.connections_limit = connections_limit
        self.memory_per_broker = memory_per_broker


kiB = 1024
MiB = kiB * kiB
GiB = MiB * kiB

# yapf: disable
AdvertisedTierConfigs = {
    #    +- ingress_rate
    #    |        +- egress_rate
    #    |        |        +- num_brokers
    #    |        |        |   +- segment_size
    #    |        |        |   |         +- cloud_cache_size
    #    |        |        |   |         |          +- partitions_min
    #    |        |        |   |         |          |   +- partitions_max
    #    |        |        |   |         |          |   |      +- connections_limit
    #    |        |        |   |         |          |   |      |     +- memory_per_broker
    #    |        |        |   |         |          |   |      |     |
    CloudTierName.AWS_1: AdvertisedTierConfig(
         20*MiB,  60*MiB,  3,  512*MiB,  300*GiB,   20, 1000,  1500, 32*GiB
    ),
    CloudTierName.AWS_2: AdvertisedTierConfig(
         50*MiB, 150*MiB,  3,  512*MiB,  500*GiB,   50, 2000,  3750, 64*GiB
    ),
    CloudTierName.AWS_3: AdvertisedTierConfig(
        100*MiB, 200*MiB,  6,  512*MiB,  500*GiB,  100, 5000,  7500, 64*GiB
    ),
    CloudTierName.AWS_4: AdvertisedTierConfig(
        200*MiB, 400*MiB,  6,    1*GiB, 1000*GiB,  100, 5000, 15000, 96*GiB
    ),
    CloudTierName.AWS_5: AdvertisedTierConfig(
        400*MiB, 800*MiB,  9,    1*GiB, 1000*GiB,  150, 7500, 30000, 96*GiB
    ),
    CloudTierName.GCP_1: AdvertisedTierConfig(
         20*MiB,  60*MiB,  3,  512*MiB,  150*GiB,   20,  500,  1500,  16*GiB
    ),
    CloudTierName.GCP_2: AdvertisedTierConfig(
         50*MiB, 150*MiB,  3,  512*MiB,  300*GiB,   50, 1000,  3750, 32*GiB
    ),
    CloudTierName.GCP_3: AdvertisedTierConfig(
        100*MiB, 200*MiB,  6,  512*MiB,  320*GiB,  100, 3000,  7500, 32*GiB
    ),
    CloudTierName.GCP_4: AdvertisedTierConfig(
        200*MiB, 400*MiB,  9,  512*MiB,  350*GiB,  100, 5000, 15000, 32*GiB
    ),
    CloudTierName.GCP_5: AdvertisedTierConfig(
        400*MiB, 600*MiB, 12,    1*GiB,  750*GiB,  100, 7500, 22500, 32*GiB
    ),
}
# yapf: enable


class RpCloudApiClient(object):
    def __init__(self, config, log):
        self._config = config
        self._token = None
        self._logger = log
        self.lasterror = None

    def _handle_error(self, response):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            self.lasterror = f'{e} {response.text}'
            self._logger.error(self.lasterror)
            return None
        return response

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
            _r = self._handle_error(resp)
            if _r is None:
                return _r
            j = resp.json()
            self._token = j['access_token']
        return self._token

    def _http_get(self, endpoint='', base_url=None, **kwargs):
        token = self._get_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
        _base = base_url if base_url else self._config.api_url
        resp = requests.get(f'{_base}{endpoint}', headers=headers, **kwargs)
        _r = self._handle_error(resp)
        return _r if _r is None else _r.json()

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
        _r = self._handle_error(resp)
        return _r if _r is None else _r.json()

    def _http_delete(self, endpoint='', **kwargs):
        token = self._get_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
        resp = requests.delete(f'{self._config.api_url}{endpoint}',
                               headers=headers,
                               **kwargs)
        _r = self._handle_error(resp)
        return _r if _r is None else _r.json()


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
    gcp_keyfile: str = ""
    region: str = "us-west-2"
    provider: str = "AWS"
    type: str = "FMC"
    network: str = "public"
    config_profile_name: str = "default"

    install_pack_ver: str = "latest"
    install_pack_url_template: str = ""
    install_pack_auth_type: str = ""
    install_pack_auth: str = ""


@dataclass
class LiveClusterParams:
    """
    Active Cluster params.
    Should not be used outside of the redpanda_cloud module
    """
    _isAlive: bool = False
    connection_type: str = 'public'
    namespace_uuid: str = None
    name: str = None
    consoleUrl: str = ""
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
    vpc_peering_id: str = None

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
        # Provider specific actions
        if self.config.provider not in [PROVIDER_AWS, PROVIDER_GCP]:
            raise RuntimeError(f"Provider '{self.config.provider}' "
                               "is not yet supported by CloudV2")

        if self.config.provider == PROVIDER_AWS:
            self.provider_key = provider_config['access_key']
            self.provider_secret = provider_config['secret_key']
        elif self.config.provider == PROVIDER_GCP:
            self.provider_key = self.config.gcp_keyfile
            self.provider_secret = None
        # Create client for the provider
        self.provider_cli = make_provider_client(self.config.provider, logger,
                                                 self.config.region,
                                                 self.provider_key,
                                                 self.provider_secret)
        if self.config.network != 'public':
            self._ducktape_meta = self.get_ducktape_meta()
            if self.config.provider == PROVIDER_AWS:
                # We should have only 1 interface on ducktape client
                self.current.peer_vpc_id = self._ducktape_meta[
                    'network-interfaces-macs-0-vpc-id']
                self.current.peer_owner_id = self._ducktape_meta[
                    'network-interfaces-macs-0-owner-id']
            elif self.config.provider == PROVIDER_GCP:
                # In case of GCP, we should have full URL not just id
                _net = self.provider_cli.get_vpc_by_network_id(
                    self._ducktape_meta['network-interfaces-0-network'].split(
                        '/')[-1],
                    prefix="")
                self.current.peer_vpc_id = _net[self.provider_cli.VPC_ID_LABEL]
                self.current.peer_owner_id = self.provider_cli.project_id

            # Currently we need provider client only for VCP in private networking
            # Raise exception is client in not implemented yet
            if self.provider_cli is None and self.config.network != 'public':
                self._logger.error(
                    f"Current provider is not yet supports private networking "
                )
                raise RuntimeError("Private networking is not implemented "
                                   f"for '{self.config.provider}'")

    @property
    def cluster_id(self):
        """
        The clusterId of the created cluster.
        """

        return self.config.id

    def get_ducktape_meta(self):
        """
        Returns instance metadata based on current provider
        This is placed in separate function to be able 
        to add data processing if needed
        """
        return self.provider_cli.get_instance_meta()

    @property
    def isAlive(self):
        _c = self._get_cluster(self.config.id)
        self.current._isAlive = True if _c['status'][
            'health'] == 'healthy' else False
        return self.current._isAlive

    def _get_cloud_users(self):
        _users = []
        _offset = 0
        _total = 1
        while _offset < _total:
            _params = {'offset': _offset}
            _r = self.cloudv2._http_get(endpoint="/api/v1/users",
                                        params=_params)
            if _r is None:
                return {}
            _users += _r['users']['results']
            _offset += len(_r['users']['results'])
            _total = _r['users']['total']
        return _users

    def _get_cluster_users(self):
        _r = self.cloudv2._http_get(
            base_url=self.current.consoleUrl,
            endpoint="/api/users",
        )
        if _r is None:
            return []
        else:
            return _r['users']

    def cloudUserExists(self, username):
        _users = self._get_cloud_users()
        if not _users:
            return False
        else:
            _usernames = [u['name'] for u in _users]
            return False if username not in _usernames else True

    def clusterUserExists(self, username):
        _users = self._get_cluster_users()
        if not _users:
            return False
        else:
            return False if username not in _users else True

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

    def _get_cluster_console_url(self):
        cluster = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.config.id}')
        return cluster['status']['listeners']['redpandaConsole']['default'][
            'urls'][0]

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

        :return: version, e.g. '23.2.20230707135118', or None if not found
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
        self.current._isAlive = True if _c['status'][
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

    def update_cluster_acls(self, superuser):
        if superuser is not None and not self.clusterUserExists(
                superuser.username):
            self._logger.debug(f'super username: {superuser.username}, '
                               f'algorithm: {superuser.algorithm}')
            self._create_user(superuser)
            self._create_acls(superuser.username)

        return

    def create(self, superuser: Optional[SaslCredentials] = None) -> str:
        """Create a cloud cluster and a new namespace; block until cluster is finished creating.

        :param config_profile_name: config profile name, default 'tier-1-aws'
        :return: clusterId, e.g. 'cimuhgmdcaa1uc1jtabc'
        """

        if not self.isPublicNetwork:
            self.current.connection_type = 'private'
        # Handle default values
        if self.config.config_profile_name == 'default':
            self.config.config_profile_name = TIER_DEFAULTS[
                self.config.provider]

        if self.config.id != '':
            # Cluster already exist
            # Just load needed info to create peering
            self._logger.warn('will not create cluster; already have '
                              f'cluster_id {self.config.id}')
            # Populate self.current from cluster info
            self._update_live_cluster_info()
            # Fill in additional info based on collected from cluster
            self.current.product_id = self._get_product_id(
                self.config.config_profile_name)
        else:
            # In order not to have long list of arguments in each internal
            # functions, use self.current as a data store
            # Each of the _*_payload functions and _get_* will use it as a source
            # of data to create proper body for REST request
            self.current.namespace_uuid = self._create_namespace()
            # name rp-ducktape-cluster-3b36f516
            self.current.name = f'rp-ducktape-cluster-{self._unique_id}'
            # Install pack handling
            if self.config.install_pack_ver == 'latest':
                self.config.install_pack_ver = \
                    self._get_latest_install_pack_ver()
            self.current.install_pack_ver = self.config.install_pack_ver
            # Multi-zone not supported, so get a single one from the list
            self.current.region = self.config.region
            self.current.region_id = self._get_region_id()
            self.current.zones = self.provider_cli.get_single_zone(
                self.current.region)
            # Call CloudV2 API to determine Product ID
            self.current.product_id = self._get_product_id(
                self.config.config_profile_name)
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

            # handle error on CloudV2 side
            if r is None:
                raise RuntimeError(self.cloudv2.lasterror)

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

        # Update Cluster console Url
        self.current.consoleUrl = self._get_cluster_console_url()
        # update cluster ACLs
        self.update_cluster_acls(superuser)

        # If network type is private, trigget VPC Peering
        if not self.isPublicNetwork:
            self.create_vpc_peering()
        self.current._isAlive = True
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
        payload = {
            'mechanism': user.algorithm,
            'password': user.password,
            'username': user.username,
        }
        # use the console api url to create sasl users; uses the same auth token
        return self.cloudv2._http_post(base_url=self.current.consoleUrl,
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

    def _create_network_peering_payload_aws(self):
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

    def _create_network_peering_payload_gcp(self):
        return {
            "networkPeering": {
                "displayName": f'peer-{self.current.name}',
                "spec": {
                    "provider": "GCP",
                    "cloudProvider": {
                        "gcp": {
                            "peerProjectId": self.current.peer_owner_id,
                            "peerVpcName":
                            self.current.peer_vpc_id.split('/')[-1]
                        }
                    }
                }
            },
            "namespaceUuid": self.current.namespace_uuid
        }

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
        if self.config.provider == PROVIDER_AWS:
            self.current.rp_vpc_id = _info['vpcId']
            self.current.rp_owner_id = _info['ownerId']
            self.current.network_cidr = _info['cidrBlock']
        elif self.config.provider == PROVIDER_GCP:
            self.current.rp_vpc_id = _info['networkName']
            self.current.rp_owner_id = _info['projectId']
            self.current.network_cidr = self.config.network

    def _prepare_byoc_network_vpc_info(self):
        # Network is handled by provider
        _net = self.provider_cli.get_vpc_by_network_id(self.current.network_id)
        self.current.rp_vpc_id = _net[self.provider_cli.VPC_ID_LABEL]
        self.current.rp_owner_id = _net[self.provider_cli.OWNER_ID_LABEL]
        self.current.network_cidr = _net[self.provider_cli.CIDR_LABEL]

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
            self.current.vpc_peering_id, state),
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
                self.current.vpc_peering_id)

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
                self.current.vpc_peering_id)

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
        if self.config.provider == PROVIDER_AWS:
            _body = self._create_network_peering_payload_aws()
            self._logger.debug(f"body: '{_body}'")

            # Create peering
            resp = self.cloudv2._http_post(
                endpoint=self.current.network_endpoint, json=_body)
            if resp is None:
                # Check if such peering exists
                self._logger.warning(self.cloudv2.lasterror)
                if "network peering already exists" in self.cloudv2.lasterror:
                    self.vpc_peering = self.cloudv2._http_get(
                        endpoint=self.current.network_endpoint)[0]
                    self.current.vpc_peering_id = self.vpc_peering
                    # State should be ready at this point
                    self._logger.warning(
                        "Found VPC peering connection "
                        f"'{self.vpc_peering['displayName']}', "
                        f"state '{self.vpc_peering['state']}'")
                    return
                else:
                    raise RuntimeError(self.cloudv2.lasterror)
            else:
                self._logger.debug(f"Created VPC peering: '{resp}'")
                self.vpc_peering = resp

            # 3.
            # Wait for "pending acceptance"
            self._wait_peering_status_cluster("pending acceptance")

            # Find id of the correct peering VPC
            self.current.vpc_peering_id = self.provider_cli.find_vpc_peering_connection(
                "pending-acceptance", self.current)

            # 4. Accept it on AWS
            self.current.aws_vpc_peering = self.provider_cli.accept_vpc_peering(
                self.current.vpc_peering_id)

            # 5.
            self._create_routes_to_ducktape()
            self._create_routes_to_cluster()

            # 6.
            self._wait_peering_status_provider("active")
            self._wait_peering_status_cluster("ready")
        elif self.config.provider == PROVIDER_GCP:
            # In scope of GCP, VCP peerings should face each other
            # But for FMC, there might be no access to cluster project
            # so use CloudV2 REST handle
            # RP -> Ducktape
            _body = self._create_network_peering_payload_gcp()
            self._logger.debug(f"body: '{_body}'")

            # Create peering
            resp = self.cloudv2._http_post(
                endpoint=self.current.network_endpoint, json=_body)
            self._logger.debug(f"Created VPC peering: '{resp}'")
            self.vpc_peering = resp

            # Ducktape -> RP
            # facing_vpcs=False turns off RP peering creation
            # that was created already above
            self.current.vpc_peering_id = self.provider_cli.create_vpc_peering(
                self.current, facing_vpcs=False)

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
        # GCP will fall to error if such peering exists
        # Will just return existing one
        self.current.vpc_peering_id = self.provider_cli.create_vpc_peering(
            self.current)

        if self.config.provider == PROVIDER_AWS:
            # 3.
            # Check if this is active already
            _is_active = self._check_peering_status_provider(
                self.current.vpc_peering_id, "active")
            if not _is_active:
                self._wait_peering_status_provider("pending-acceptance")
                # 4.
                # If this is freshly created, accept it
                self.current.aws_vpc_peering = self.provider_cli.accept_vpc_peering(
                    self.current.vpc_peering_id)
            else:
                self.current.aws_vpc_peering = \
                    self.provider_cli.get_vpc_peering_connection(
                        self.current.vpc_peering_id)

            # 5.
            if self.config.provider == PROVIDER_AWS:
                # Only AWS needs route creation
                # GCP does this by default
                self._create_routes_to_ducktape()
                self._create_routes_to_cluster()

            # 6.
            # No need to wait if its active already
            if not _is_active:
                self._wait_peering_status_provider('active')
        elif self.config.provider == PROVIDER_GCP:
            # In case of GCP nothing more is needed
            pass

        return

    def create_vpc_peering(self):
        """
        Create VPC peering for deployed cloud with private network.
        Detect cloud type and run corresponding workflow

        """
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
