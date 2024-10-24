import base64
import collections
from functools import cache
import json
import os
import requests
import uuid
import yaml
import ipaddress
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from prometheus_client.parser import text_string_to_metric_families

from ducktape.utils.util import wait_until
from rptest.services.cloud_cluster_utils import CloudClusterUtils
from rptest.services.provider_clients import make_provider_client
from rptest.services.provider_clients.ec2_client import RTBS_LABEL
from rptest.services.provider_clients.rpcloud_client import RpCloudApiClient
from urllib.parse import urlparse

from rptest.services.redpanda_types import SaslCredentials

ns_name_prefix = "rp-ducktape-ns-"
ns_name_date_fmt = "%Y-%m-%d-%H%M%S-"

CLOUD_TYPE_FMC = 'FMC'
CLOUD_TYPE_BYOC = 'BYOC'
PROVIDER_AWS = 'AWS'
PROVIDER_GCP = 'GCP'
PROVIDER_AZURE = 'AZURE'

TIER_DEFAULTS = {
    PROVIDER_AWS: "tier-1-aws",
    PROVIDER_GCP: "tier-1-gcp",
    PROVIDER_AZURE: "tier-1-azure-v2-x86"
}


def get_config_profile_name(config: None | dict[str, Any]) -> str:
    """Gets config profile name

    Suitable to call before cluster creation.

    :return: string name of config profile
    """
    if not config:
        return 'docker-local'

    if config['config_profile_name'] == 'default':
        _provider = config['provider'].upper()
        return TIER_DEFAULTS[_provider]

    return config['config_profile_name']


class CloudTierName(Enum):
    AWS_1 = 'tier-1-aws'
    AWS_2 = 'tier-2-aws'
    AWS_3 = 'tier-3-aws'
    AWS_4 = 'tier-4-aws'
    AWS_5 = 'tier-5-aws'
    AWS_1_P5 = 'tco-p5-tier-1-aws'
    AWS_2_P5 = 'tco-p5-tier-2-aws'
    AWS_3_P5 = 'tco-p5-tier-3-aws'
    AWS_4_P5 = 'tco-p5-tier-4-aws'
    AWS_5_P5 = 'tco-p5-tier-5-aws'
    AWS_6_P5 = 'tco-p5-tier-6-aws'
    AWS_7_P5 = 'tco-p5-tier-7-aws'
    AWS_1_P5_ARM = 'tco-p5-tier-1-aws-arm'
    AWS_2_P5_ARM = 'tco-p5-tier-2-aws-arm'
    AWS_3_P5_ARM = 'tco-p5-tier-3-aws-arm'
    AWS_4_P5_ARM = 'tco-p5-tier-4-aws-arm'
    AWS_5_P5_ARM = 'tco-p5-tier-5-aws-arm'
    AWS_6_P5_ARM = 'tco-p5-tier-6-aws-arm'
    AWS_7_P5_ARM = 'tco-p5-tier-7-aws-arm'
    GCP_1 = 'tier-1-gcp'
    GCP_2 = 'tier-2-gcp'
    GCP_3 = 'tier-3-gcp'
    GCP_4 = 'tier-4-gcp'
    GCP_5 = 'tier-5-gcp'
    GCP_1_P5 = 'tco-p5-tier-1-gcp'
    GCP_2_P5 = 'tco-p5-tier-2-gcp'
    GCP_3_P5 = 'tco-p5-tier-3-gcp'
    GCP_4_P5 = 'tco-p5-tier-4-gcp'
    GCP_5_P5 = 'tco-p5-tier-5-gcp'
    GCP_6_P5 = 'tco-p5-tier-6-gcp'
    GCP_7_P5 = 'tco-p5-tier-7-gcp'
    AZURE_1 = 'tier-1-azure-v2-x86'
    AZURE_2 = 'tier-2-azure-v2-x86'
    AZURE_3 = 'tier-3-azure-v2-x86'
    AZURE_4 = 'tier-4-azure-v2-x86'
    AZURE_5 = 'tier-5-azure-v2-x86'
    AZURE_6 = 'tier-6-azure-v2-x86'
    AZURE_7 = 'tier-7-azure-v2-x86'
    AZURE_8 = 'tier-8-azure-v2-x86'
    AZURE_9 = 'tier-9-azure-v2-x86'

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


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
    admin_api_url: str = ""
    public_api_url: str = ""
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
    use_same_cluster: bool = True
    install_pack_ver: str = "latest"
    install_pack_url_template: str = ""
    install_pack_auth_type: str = ""
    install_pack_auth: str = ""
    grafana_token: str = ""
    grafana_alerts_url: str = ""
    require_broker_metrics_in_health_check: bool = True


@dataclass
class LiveClusterParams:
    """
    Active Cluster params.
    Should not be used outside of the redpanda_cloud module

    DO NOT USE outside this module. To expose any paramteter,
    create property method in CloudCluster class
    """
    cluster_id: str = ""
    _isAlive: bool = False
    connection_type: str = 'public'
    namespace_uuid: str | None = None
    name: str | None = None
    last_status: str = ""
    consoleUrl: str = ""
    network_id: str | None = None
    network_cidr: str | None = None
    install_pack_ver: str | None = None
    product_name: str | None = None
    region: str | None = None
    region_id: str | None = None
    peer_vpc_id: str | None = None
    peer_owner_id: str | None = None
    rp_vpc_id: str | None = None
    rp_owner_id: str | None = None
    vpc_peering_id: str | None = None
    # Special flag that is checked on cluster deletion
    # last test in the list should set this to True
    # along with 'use_same_cluster' in config
    # this will initiane cluster deletion
    tests_finished: bool = False
    # Can't use mutables in defaults of dataclass
    # https://docs.python.org/3/library/dataclasses.html#dataclasses.field
    zones: list[str] = field(default_factory=list)
    aws_vpc_peering: dict[str, Any] = field(default_factory=dict)
    # TODO Azure vpc peering was not yet tested since Azure cluster creation on cloud with Tiers is blocked. This part might change
    azure_vpc_peering: dict[str, Any] = field(default_factory=dict)

    @property
    def network_endpoint(self):
        return f'/api/v1/networks/{self.network_id}/network-peerings'


@dataclass
class ProductInfo:
    max_ingress: int
    max_egress: int
    max_connection_count: int
    max_partition_count: int


class CloudCluster():
    """
    Operations on a Redpanda Cloud cluster via the swagger API.

    Creates and deletes a cluster. Will also create a new namespace for
    that cluster.
    """
    _cid_filename = '.cluster_id'

    CHECK_TIMEOUT_SEC = 3600
    CHECK_BACKOFF_SEC = 60.0

    def __init__(self,
                 context,
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
        # Handle default value for tier
        if self.config.config_profile_name == 'default':
            self.config.config_profile_name = TIER_DEFAULTS[
                self.config.provider]
        # Init API client
        self.cloudv2 = RpCloudApiClient(self.config, logger)

        # copy the regular config, but override the api_url to point
        # to the public API instead
        public_config = CloudClusterConfig(**cluster_config)
        public_config.api_url = public_config.public_api_url
        self.public_api = RpCloudApiClient(public_config, logger)

        # Create helper bool variable
        self.isPublicNetwork = self.config.network == 'public'

        # unique 8-char identifier to be used when creating names of things
        # for this cluster
        self._unique_id = str(uuid.uuid1())[:8]

        # init live cluster params
        self.current = LiveClusterParams()
        # Provider specific actions
        if self.config.provider not in [
                PROVIDER_AWS, PROVIDER_GCP, PROVIDER_AZURE
        ]:
            raise RuntimeError(f"Provider '{self.config.provider}' "
                               "is not yet supported by CloudV2")

        if self.config.provider == PROVIDER_AWS:
            self.provider_key = provider_config['access_key']
            self.provider_secret = provider_config['secret_key']
            self.provider_tenant = None
        elif self.config.provider == PROVIDER_GCP:
            self.provider_key = self.config.gcp_keyfile
            self.provider_secret = None
            self.provider_tenant = None
        elif self.config.provider == PROVIDER_AZURE:
            self.provider_key = provider_config['azure_client_id']
            self.provider_secret = provider_config['azure_client_secret']
            self.provider_tenant = provider_config['azure_tenant_id']
        # Create client for the provider
        self.provider_cli = make_provider_client(self.config.provider,
                                                 logger,
                                                 self.config.region,
                                                 self.provider_key,
                                                 self.provider_secret,
                                                 tenant=self.provider_tenant)
        if self.config.network != 'public':
            # Check that private network is a correct CIDR
            self.config.network = self.validate_cidr(self.config.network)
            # Get all metadata from ducktape runner node
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
            elif self.config.provider == PROVIDER_AZURE:
                # For Azure, retrieve VNet and Subscription ID
                self.current.peer_vpc_id = self._ducktape_meta[
                    'network-interfaces-0-vnet-id']
                self.current.peer_owner_id = self._ducktape_meta[
                    'subscription-id']

            # Currently we need provider client only for VCP in private networking
            # Raise exception if client is not implemented yet
            if self.provider_cli is None and self.config.network != 'public':
                self._logger.error(
                    f"Current provider does not yet support private networking"
                )
                raise RuntimeError("Private networking is not implemented "
                                   f"for '{self.config.provider}'")

        # prepare rpk plugin
        o = urlparse(self.config.oauth_url)
        oauth_url_origin = f'{o.scheme}://{o.hostname}'
        self.utils = CloudClusterUtils(context, self._logger,
                                       self.provider_key, self.provider_secret,
                                       self.config.provider,
                                       self.config.api_url, oauth_url_origin,
                                       self.config.oauth_audience)
        if self.config.type == CLOUD_TYPE_BYOC:
            # remove current plugin if any
            self.utils.rpk_plugin_uninstall('byoc', sudo=True)
            self.utils.rpk_plugin_uninstall('byoc')

        # save context
        self._ctx = context

    def validate_cidr(self, network_cidr):
        try:
            ip_address = ipaddress.ip_network(network_cidr)
        except ValueError as e:
            raise RuntimeError(
                f"Invalid CIDR for private network: '{network_cidr}'") from e
        return str(ip_address)

    @property
    def cluster_id(self):
        """
        The clusterId of the created cluster.
        """
        if self.current._isAlive:
            return self.current.cluster_id
        else:
            return None

    def get_ducktape_meta(self):
        """
        Returns instance metadata based on current provider
        This is placed in separate function to be able
        to add data processing if needed
        """
        return self.provider_cli.get_instance_meta()

    @property
    def isAlive(self):
        _c = self._get_cluster(self.current.cluster_id)
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

    def _format_namespace_name(self):
        # format namespace name as 'rp-ducktape-ns-YYYY-MM-DD-HHMMSS-3b36f516'
        _date = datetime.now().strftime(ns_name_date_fmt)
        # For easier regex parsing, date format has second dash inside
        return f'{ns_name_prefix}{_date}{self._unique_id}'

    def _create_namespace(self):
        # fetch namespace name
        name = self._format_namespace_name()
        self._logger.debug(f'creating namespace name {name}')
        body = {'name': name}
        # namespace, resource-group… totally the same thing
        r = self.public_api._http_post(endpoint='/v1beta2/resource-groups',
                                       json=body)
        _id = r['resource_group']['id']
        self._logger.debug(f"created namespaceUuid {_id}")
        # save namespace name
        self.config.namespace = name
        return _id

    def _cluster_ready(self):
        # Get cluster info
        try:
            c = self._get_cluster(self.current.cluster_id)
        except Exception as e:
            # Consider it non critical and try again later
            self._logger.warning(f"Failed to get cluster info: {e}")
            return False
        # Check state and raise error if anything critical happens
        self._logger.debug(f"Cluster status: {c['state']}")
        self.current.last_status = c['state']
        if c['state'] == 'ready':
            return True
        elif c['state'] == 'unknown':
            raise RuntimeError("Creation failed (state 'unknown') "
                               f"for '{self.config.provider}'")
        elif c['state'] == 'deleting':
            raise RuntimeError("Creation failed (state 'deleting') "
                               f"for '{self.config.provider}'")

        return False

    def _cluster_status(self, status):
        _cluster = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.current.cluster_id}')
        return _cluster['state'] == status

    def _get_cluster_console_url(self):
        cluster = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.current.cluster_id}')
        return cluster['status']['listeners']['redpandaConsole']['default'][
            'urls'][0]

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

        :param cloudProvider: cloud provider, e.g. 'CLOUD_PROVIDER_AWS'
        :param name: region name, e.g. 'us-west-2'
        :return: id, e.g. 'cckac9vvbr5ofm048jjg'
        """

        provider = f'CLOUD_PROVIDER_{self.config.provider}'
        body = {'cloudProvider': provider, 'name': self.current.region}
        regions = self.public_api._http_post(
            endpoint='/redpanda.api.ui.v1alpha1.RegionService/ListRegions',
            override_headers={'connect-protocol-version': '1'},
            json=body)

        return next(
            filter(lambda r: r['name'] == self.current.region,
                   regions['regions']), {'id': None})['id']

    def _get_product_name(self, config_profile_name):
        """Get the product name for the first matching config
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
                return p['name']
        self._logger.warning("Could not find product for install pack, "
                             f"request: '{params}', response:\n{products}")
        return None

    def _create_network_payload(self):
        _net = self.config.network,
        _provider = f"CLOUD_PROVIDER_{self.config.provider.upper()}"
        _type = 'TYPE_BYOC' if self.config.type == 'BYOC' else 'TYPE_DEDICATED'
        # In case of private network, the value of config.network
        # should be a CIDR, but we're validating that in __init__
        return {
            "cidr_block": "10.1.0.0/16" if self.isPublicNetwork else _net,
            "cloud_provider": _provider,
            "cluster_type": _type,
            "name": f"{self.current.name}-network",
            "region": self.config.region,
            "resource_group_id": self.current.namespace_uuid
        }

    def _create_cluster_payload(self):
        _conn_type = f"CONNECTION_TYPE_{self.current.connection_type.upper()}"
        _provider = f"CLOUD_PROVIDER_{self.config.provider.upper()}"
        _type = 'TYPE_BYOC' if self.config.type == 'BYOC' else 'TYPE_DEDICATED'
        return {
            "cloud_provider": _provider,
            "connection_type": _conn_type,
            "name": self.current.name,
            "network_id": self.current.network_id,
            "region": self.config.region,
            "resource_group_id": self.current.namespace_uuid,
            "throughput_tier": self.current.product_name,
            "type": _type,
            "zones": self.current.zones
        }

    def _get_cluster(self, _id) -> dict[str, Any]:
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
        _c = self._get_cluster(self.current.cluster_id)
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

    def get_public_metrics(self):
        """Public metrics endpoint for prometheus text format.

        :return: string or None if failure
        """

        cluster = self._get_cluster(self.current.cluster_id)
        base_url = cluster['status']['listeners']['redpandaConsole'][
            'default']['urls'][0]
        username = cluster['spec']['consolePrometheusCredentials']['username']
        password = cluster['spec']['consolePrometheusCredentials']['password']
        b64 = base64.b64encode(bytes(f'{username}:{password}', 'utf-8'))
        token = b64.decode('utf-8')
        headers = {'Authorization': f'Basic {token}'}
        return self.cloudv2._http_get(
            endpoint=f'/api/cloud/prometheus/public_metrics',
            base_url=base_url,
            override_headers=headers,
            text_response=True)

    def update_cluster_acls(self, superuser):
        if superuser is not None and not self.clusterUserExists(
                superuser.username):
            self._logger.debug(f'super username: {superuser.username}, '
                               f'algorithm: {superuser.algorithm}')
            self._create_user(superuser)
            self._create_acls(superuser.username)

        return

    @property
    def _cid_file(self):
        return os.path.join(self._ctx.session_context.results_dir,
                            self._cid_filename)

    def _cluster_id_updated(self, uuid):
        _cluster = self.cloudv2._http_get(endpoint=f'/api/v1/clusters/{uuid}')
        return _cluster['id'] != uuid

    def rm_cluster_id_file(self):
        """
            Remove cluster id file in case of failure
        """
        if os.path.exists(self._cid_file):
            os.remove(self._cid_file)
        return

    def save_cluster_id(self, cluster_id):
        """
        Save cluster id to results folder for next test to use
        """
        with open(self._cid_file, 'w') as cf:
            cf.write(cluster_id)
        return

    def safe_load_cluster_id(self):
        """
        Checks if cluster_id is saved by previous test and loads it
        """
        _id = None
        if os.path.exists(self._cid_file):
            with open(self._cid_file, 'r') as cf:
                _id = cf.read()
        return _id

    def _netop_complete(self, netop_id: str, target: str) -> bool:
        n = self.public_api._http_get(
            endpoint=f'/v1beta2/operations/{netop_id}')
        if n is None:
            return False
        if 'operation' not in n or 'state' not in n['operation']:
            return False
        self._logger.debug(
            f"reached target state: {n['operation']['state'] == target}")
        return n['operation']['state'] == target

    def _wait_for_netop_id(self,
                           netop_id,
                           timeout=300,
                           target='STATE_COMPLETED') -> str:
        self._logger.debug(f'polling /v1beta2/operations/{netop_id}')
        wait_until(lambda: self._netop_complete(netop_id, target) == True,
                   timeout_sec=timeout,
                   backoff_sec=10,
                   err_msg='Failed to get proper id '
                   f'of cloud cluster {self.current.name}')
        n = self.public_api._http_get(
            endpoint=f'/v1beta2/operations/{netop_id}')
        if n is None or 'operation' not in n or 'resource_id' not in n[
                'operation']:
            return ""
        return n['operation']['resource_id']

    def _create_new_cluster(self):
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
        self.current.zones = [
            self.provider_cli.get_single_zone(self.current.region)
        ]
        # Call CloudV2 API to determine Product ID
        self.current.product_name = self._get_product_name(
            self.config.config_profile_name)
        if self.current.product_name is None:
            raise RuntimeError("ProductID failed to be determined for "
                               f"'{self.config.provider}', "
                               f"'{self.config.type}', "
                               f"'{self.config.install_pack_ver}', "
                               f"'{self.config.region}'")

        # Call public API to create network
        self._logger.warning(
            f'creating network name "{self.current.name}-network"')
        # Prepare network payload block
        _body = self._create_network_payload()
        self._logger.debug(
            f'POST to /v1beta2/networks body: {json.dumps(_body)}')
        # Send API request to create network
        n = self.public_api._http_post(endpoint='/v1beta2/networks',
                                       json=_body)
        if n is None:
            raise RuntimeError(self.cloudv2.lasterror)
        netop_id = n['operation']['id']
        self.current.network_id = self._wait_for_netop_id(netop_id)

        # Call public API to create cluster
        self._logger.warning(f'creating cluster name {self.current.name}')
        # Prepare cluster payload block
        _body = self._create_cluster_payload()
        self._logger.debug(
            f'POST to /v1beta2/clusters body: {json.dumps(_body)}')
        # Send API request to create cluster
        r = self.public_api._http_post(endpoint='/v1beta2/clusters',
                                       json=_body)
        # handle error on CloudV2 side
        if r is None:
            raise RuntimeError(self.cloudv2.lasterror)
        netop_id = r['operation']['id']

        try:
            _cluster_id = self._wait_for_netop_id(netop_id,
                                                  target='STATE_IN_PROGRESS')
            c = self._get_cluster(_cluster_id)
            self.current.last_status = c['state']
            self._logger.warning(f"Cluster ID is {_cluster_id}, last status: "
                                 f"'{self.current.last_status}'")
        except Exception as e:
            raise RuntimeError("Failed to get initial cluster spec") from e

        # In case of BYOC cluster, do some additional stuff to create it
        if self.config.type == CLOUD_TYPE_BYOC:
            # Handle byoc creation
            # Login without saving creds
            self.utils.rpk_cloud_login(self.config.oauth_client_id,
                                       self.config.oauth_client_secret)
            # save cluster id so we can delete it in case of failure
            self.current.cluster_id = _cluster_id
            # Kick off cluster creation
            # Timeout for this is half an hour as this is only agent
            self.utils.rpk_cloud_byoc_install(_cluster_id)
            self.utils.rpk_cloud_apply(_cluster_id)
        elif self.config.type == CLOUD_TYPE_FMC:
            # Nothing to do here
            pass
        else:
            raise RuntimeError("Cloud type not supported: "
                               f"'{self.config.type}'")

        self.current.cluster_id = _cluster_id
        # In case of FMC, just poll the cluster and wait when ready
        # Poll API and wait for the cluster creation
        # Announce wait
        self._logger.info(
            f'waiting for creation of cluster {self.current.name} '
            f'({self.current.cluster_id}), namespaceUuid {self.current.namespace_uuid},'
            f' checking every {self.CHECK_BACKOFF_SEC} seconds')
        wait_until(lambda: self._cluster_ready(),
                   timeout_sec=self.CHECK_TIMEOUT_SEC,
                   backoff_sec=self.CHECK_BACKOFF_SEC,
                   err_msg='Unable to determine readiness '
                   f'of cloud cluster {self.current.name}; '
                   f'last state {self.current.last_status}')

        # at this point cluster is ready
        # just save the id to reuse it in next test
        if self.config.use_same_cluster:
            self.save_cluster_id(self.current.cluster_id)

        self._logger.warning(
            f"Cloud cluster {_cluster_id} created successfully.")

        return

    @cache
    def panda_proxy_url(self):
        cluster = self._get_cluster(self.current.cluster_id)
        return cluster['status']['listeners']['pandaProxy']['panda-proxy'][
            'urls'][0]

    def _query_panda_proxy(self, path):
        # Prepare credentials
        _u = self._superuser.username
        _p = self._superuser.password
        b64 = base64.b64encode(bytes(f'{_u}:{_p}', 'utf-8'))
        token = b64.decode('utf-8')
        headers = {'Authorization': f'Basic {token}'}
        return self.cloudv2._http_get(path,
                                      base_url=self.panda_proxy_url(),
                                      override_headers=headers)

    def get_brokers(self):
        """Get the list of brokers from the pandaproxy API."""
        return self._query_panda_proxy("/brokers")['brokers']

    def _ensure_cluster_health(self) -> str | None:
        """
            Check if current cluster is healthy
              - check connectivity
              - query health data
              - list topics

            Returns a string describing the problem if a check fails or
            None otherwise.
        """
        def warn_and_return(msg: str):
            self._logger.warning(msg)
            return msg

        # Get cluster details
        try:
            self._logger.info("Getting cluster specs")
            cluster = self._get_cluster(self.current.cluster_id)
        except Exception as e:
            return warn_and_return("# Failed to get info for cluster with Id: "
                                   f"'{self.current.cluster_id}'")

        # list cluster specs
        self._logger.info(f"Cluster '{self.current.cluster_id}': "
                          f"health = '{cluster['status']['health']}', "
                          f"state = '{cluster['state']}'")

        # Check if panda-proxy is available
        if not 'panda-proxy' in cluster['status']['listeners']['pandaProxy']:
            return warn_and_return("Panda-Proxy listener is not available")
        else:
            _u = cluster['status']['listeners']['pandaProxy']['panda-proxy'][
                'urls'][0]
            self._logger.info(f"Panda-Proxy listener: '{_u}'")

        # Check that cluster is operational
        # Check brokers count
        self._logger.info("Checking cluster brokers")
        brokers = self.get_brokers()
        if len(brokers) < 3:
            return warn_and_return("Less than 3 brokers operational")
        else:
            self._logger.info(f"Console reports '{len(brokers)}' brokers")

        # Check topic count
        self._logger.info("Checking cluster topics")
        _topics = self._query_panda_proxy("/topics")
        # For Azure, connect is in development
        if self.config.provider == PROVIDER_AZURE:
            _critical = ["_schemas", "_redpanda_e2e_probe"]
            required_critical_topic_count = 2
        else:
            _critical = [
                "_schemas", "__redpanda.connectors_logs",
                "_internal_connectors_status", "_internal_connectors_configs",
                "_redpanda_e2e_probe", "_internal_connectors_offsets"
            ]
            required_critical_topic_count = 6

        _intersect = list(set(_topics) & set(_critical))
        if len(_intersect) < required_critical_topic_count:
            return warn_and_return("Cluster missing critical topics")
        else:
            _t = ', '.join(_intersect)
            self._logger.info(f"Critical topics present: '{_t}'")

        # Check brokers metric
        self._logger.info("Checking cluster public_metrics")
        _metrics = self.get_public_metrics()
        _metrics = text_string_to_metric_families(_metrics)
        _brokers_metric = None
        for _metric in _metrics:
            if _metric.name == "redpanda_cluster_brokers":
                _brokers_metric = _metric
        if _brokers_metric is None:
            if self.config.require_broker_metrics_in_health_check:
                return warn_and_return("Failed to get brokers metric")
            else:
                self._logger.info(
                    "Public metric 'redpanda_cluster_brokers' is unavailable, but it is not required."
                )
                return None
        else:
            self._logger.info("Public metric 'redpanda_cluster_brokers' "
                              "is available")

        # Get Samples
        _instances = [s.value for s in _brokers_metric.samples]
        if max(_instances) < 3:
            return warn_and_return("Prometheus reports less than 3 instances")
        else:
            self._logger.info("Prometheus samples reports maximum of "
                              f"{max(_instances)} instances")
        # All checks passed
        return None

    def _select_cluster_id(self):
        """
        Function check if previous test saved cluster id
        Also, global configuration takes priority
        so the cluster id would be loaded only once in any case
        This will work for FMC and BYOC alike
        """

        # If globals.json originally had cluster id set,
        # use it as a priority
        _id = self.config.id
        # if _id was not provided, this will trigger safe_load_cluster_id from disk
        _id = self.safe_load_cluster_id() if not _id else _id
        # if _id loaded or provided, use it in the next run
        if _id and self.config.use_same_cluster:
            # update config with new id
            return _id
        else:
            # if there is still no id, just copy what globals.json had
            return self.config.id

    def create(self, superuser: SaslCredentials) -> str:
        """Create a cloud cluster and a new namespace; block until cluster is finished creating.

        :param config_profile_name: config profile name, default 'tier-1-aws'
        :return: clusterId, e.g. 'cimuhgmdcaa1uc1jtabc'
        """

        self._superuser = superuser

        # Select cluster id for the test run
        # From this point, only self.current.cluster_id should be used
        self.current.cluster_id = self._select_cluster_id()

        # set network flag
        if not self.isPublicNetwork:
            self.current.connection_type = 'private'

        # Prepare the cluster
        if self.current.cluster_id != '':
            fail_on_unhealthy = self.current.cluster_id == self.config.id
            # Cluster already exist
            # Check if cluster is healthy
            try:
                # In case this is a provided cluster,
                # make sure that we ready for the health check.
                # Users and ACLs will not be recreated if they exists
                self.current.consoleUrl = self._get_cluster_console_url()
                self.update_cluster_acls(superuser)
                # Do the health check
                unhealthy_reason: str | None = self._ensure_cluster_health()
            except Exception as e:
                if fail_on_unhealthy:
                    raise
                msg = f"Health check ended exceptionally: {e}"
                self._logger.warning(msg)
                unhealthy_reason = msg

            if unhealthy_reason is not None:
                if fail_on_unhealthy:
                    # Cluster id was provided. Generate exception as
                    # cluster creation not logical in this case
                    raise RuntimeError(
                        "Provided Cluster with id "
                        f"'{self.config.id}' "
                        f"failed health check: {unhealthy_reason}")

                # Health check fail, create new cluster
                self._logger.warning(f"Cluster '{self.current.cluster_id}' "
                                     "not healthy, creating new")
                self.current.cluster_id = ""

                # Clean out cluster id file
                self.rm_cluster_id_file()
                # Create new cluster
                self._create_new_cluster()
            else:
                # Just load needed info to create peering
                self._logger.warning('will not create cluster; already have '
                                     f'cluster_id {self.current.cluster_id}')
                # Populate self.current from cluster info
                self._update_live_cluster_info()
                # Fill in additional info based on collected from cluster
                self.current.product_name = self._get_product_name(
                    self.config.config_profile_name)
        else:
            # Just create new cluster
            self._create_new_cluster()

        # If network type is private, trigger VPC Peering
        if not self.isPublicNetwork:
            # Checks for if such VPC exists is not needed

            # In case of AWS, it will not create
            # duplicate Peering connection, just return existing one.

            # In case of GCP, there is no need for Peering for BYOC
            # and FMC type will create proper peering if it is not in place

            # Routes detection logic is in place, so no duplicated
            # routes/tables will be created
            self.create_vpc_peering()

        # Update Cluster console Url
        self.current.consoleUrl = self._get_cluster_console_url()
        # update cluster ACLs
        self.update_cluster_acls(superuser)

        self.current._isAlive = True
        return self.current.cluster_id

    def delete(self):
        """
        Deletes a cloud cluster and the namespace it belongs to.
        Cluster delete is initiated via cluster nodes stop.
        """
        if self.current.cluster_id == '':
            self._logger.warning('cluster_id is empty, '
                                 'unable to delete cluster')
            return
        elif not self.config.delete_cluster:
            self._logger.warning('Cluster deletion skipped as configured')
            return
        elif self.config.use_same_cluster:
            # Check for tests finished flag only when same cluster used
            if not self.current.tests_finished:
                self._logger.info("Not all tests finished, skipped deletion")
                return
            else:
                pass

        self._logger.info("Deleting cluster")
        resp = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.current.cluster_id}')
        namespace_uuid = resp['namespaceUuid']

        # For FMC, just delete the cluster and the rest will happen
        # by itself

        # For BYOC cluster deletion happens in 2 phases
        # 1. Delete cluster
        # 2. Wait for status "delete agent"
        # 3. Use rpk to delete agent

        resp = self.cloudv2._http_delete(
            endpoint=f'/api/v1/clusters/{self.current.cluster_id}')
        self._logger.debug(f'resp: {json.dumps(resp)}')

        # Check if this is a BYOC and delete agent
        if self.config.type == CLOUD_TYPE_BYOC:
            wait_until(lambda: self._cluster_status('deleting_agent'),
                       timeout_sec=self.CHECK_TIMEOUT_SEC,
                       backoff_sec=self.CHECK_BACKOFF_SEC,
                       err_msg='Timeout waiting for deletion '
                       f'of cloud cluster {self.current.name}')
            # Once deleted run agent delete
            self.utils.rpk_cloud_agent_delete(self.current.cluster_id)

        # This cluster is no longer available
        self.current.cluster_id = ''
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
            endpoint=f'/api/v1/clusters/{self.current.cluster_id}')
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
            endpoint=f'/api/v1/clusters/{self.current.cluster_id}')
        return cluster['status']['listeners']['kafka']['default']['urls'][0]

    def get_install_pack_version(self):
        cluster = self.cloudv2._http_get(
            endpoint=f'/api/v1/clusters/{self.current.cluster_id}')
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

    def _create_network_peering_payload_azure(self):
        return {
            "networkPeering": {
                "displayName": f'peer-{self.current.name}',
                "spec": {
                    "provider": "AZURE",
                    "cloudProvider": {
                        "azure": {
                            "peerSubscriptionId": self.current.peer_owner_id,
                            "peerVirtualNetworkId": self.current.peer_vpc_id
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

    def _ensure_routes(self, rtables, cidr, vpc_id):
        def _route_exists(routes, _cidr):
            # Lookup CIDR and VpcId in table
            for _route in routes:
                if 'DestinationCidrBlock' in _route and \
                    _route['DestinationCidrBlock'] == _cidr and \
                    'VpcPeeringConnectionId' in _route and \
                    _route['VpcPeeringConnectionId'] == self.current.vpc_peering_id:
                    return True
            return False

        # Iterate through table and create if not exists
        for _tbl in rtables:
            # Check if this route is already exists in this table
            if _route_exists(_tbl['Routes'], cidr):
                self._logger.debug(f"Route to '{cidr}' already exists "
                                   f"in table with id '{_tbl}'")
                continue
            else:
                # Create it if not
                self.provider_cli.create_route(_tbl['RouteTableId'], cidr,
                                               vpc_id)
        return

    def _create_routes_to_ducktape(self):
        # Create routes from CloudV2 to Ducktape (10.10.xxx -> 172...)
        # aws ec2 describe-route-tables --filter "Name=tag:Name,Values=network-cjah1ecce8edpeoj0li0" "Name=tag:purpose,Values=private" | jq -r '.RouteTables[].RouteTableId' | while read -r route_table_id; do aws ec2 create-route --route-table-id $route_table_id --destination-cidr-block 172.31.0.0/16 --vpc-peering-connection-id pcx-0581b037d9e93593e; done;
        # FMC: handled by CloudV2 and ID count get_rtb results in zero
        # BYOC: Should create routes
        _rtbs = self.provider_cli.get_route_tables_for_cluster(
            self.current.network_id)
        self._ensure_routes(
            _rtbs[RTBS_LABEL],
            self.current.aws_vpc_peering['AccepterVpcInfo']['CidrBlock'],
            self.current.vpc_peering_id)
        return

    def _create_routes_to_cluster(self):
        # Create routes from Ducktape to CloudV2
        # aws ec2 --region us-west-2 create-route --route-table-id rtb-02e89e44cb4da000d --destination-cidr-block 10.10.0.0/16 --vpc-peering-connection-id pcx-0581b037d9e93593e
        _rtbs = self.provider_cli.get_route_tables_for_vpc(
            self.current.peer_vpc_id)
        self._ensure_routes(
            _rtbs[RTBS_LABEL],
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
                # self._logger.warning(self.cloudv2.lasterror)
                if "network peering already exists" in self.cloudv2.lasterror:
                    self.vpc_peering = self.cloudv2._http_get(
                        endpoint=self.current.network_endpoint)[0]
                    # State should be ready at this point
                    self._logger.warning(
                        "Found Cloud VPC peering connection "
                        f"'{self.vpc_peering['displayName']}', "
                        f"state '{self.vpc_peering['state']}'")
                    # Search for AWS peering
                    self.current.vpc_peering_id = \
                        self.provider_cli.find_vpc_peering_connection(
                            "active", self.current)
                    if self.current.vpc_peering_id is None:
                        raise RuntimeError("AWS VPC Peering connection "
                                           f"not found: {self.current}")
                    else:
                        self.current.aws_vpc_peering = \
                            self.provider_cli.get_vpc_peering_connection(
                                self.current.vpc_peering_id)
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
        elif self.config.provider == PROVIDER_AZURE:
            # TODO This part was not yet tested. Azure cloud cluster creation for Tier testing is blocked. Added this section based on AWS, but might need to update it
            # Azure specific VPC peering process
            _body = self._create_network_peering_payload_azure()
            self._logger.debug(f"body: '{_body}'")

            # Create peering
            resp = self.cloudv2._http_post(
                endpoint=self.current.network_endpoint, json=_body)
            if resp is None:
                # Check if such peering exists
                if "network peering already exists" in self.cloudv2.lasterror:
                    self.vpc_peering = self.cloudv2._http_get(
                        endpoint=self.current.network_endpoint)[0]
                    self._logger.warning(
                        "Found Cloud VPC peering connection "
                        f"'{self.vpc_peering['displayName']}', "
                        f"state '{self.vpc_peering['state']}'")
                    self.current.vpc_peering_id = \
                        self.provider_cli.find_vpc_peering_connection(
                            "active", self.current)
                    if self.current.vpc_peering_id is None:
                        raise RuntimeError("Azure VPC Peering connection "
                                           f"not found: {self.current}")
                    else:
                        self.current.azure_vpc_peering = \
                            self.provider_cli.get_vpc_peering_connection(
                                self.current.vpc_peering_id)
                else:
                    raise RuntimeError(self.cloudv2.lasterror)
            else:
                self._logger.debug(f"Created VPC peering: '{resp}'")
                self.vpc_peering = resp

                # 3. Wait for "pending acceptance"
                self._wait_peering_status_cluster("pending acceptance")

                # Find id of the correct peering VPC
                self.current.vpc_peering_id = self.provider_cli.find_vpc_peering_connection(
                    "pending-acceptance", self.current)

                # 4. Accept it on Azure
                self.current.azure_vpc_peering = self.provider_cli.accept_vpc_peering(
                    self.current.vpc_peering_id)

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

    def get_product(self) -> ProductInfo | None:
        """ Get product information.

        Returns dict with info of product, including advertised limits.
        Returns none if product info for the tier is not found.
        """

        if self.config.install_pack_ver == 'latest':
            install_pack_ver = self._get_latest_install_pack_ver()
        else:
            install_pack_ver = self.config.install_pack_ver
        params = {
            'cloud_provider': self.config.provider,
            'cluster_type': self.config.type,
            'region': self.config.region,
            'install_pack_version': install_pack_ver
        }
        products = self.cloudv2._http_get(
            endpoint='/api/v1/clusters-resources/products', params=params)
        for product in products:
            if product[
                    'redpandaConfigProfileName'] == self.config.config_profile_name:
                return ProductInfo(
                    max_ingress=int(product['advertisedMaxIngress']),
                    max_egress=int(product['advertisedMaxEgress']),
                    # note that despite the name advertisedMaxClientCount is actually
                    # the advertised connection count, which is a much different value
                    # (clients may make many connections to a single cluster)
                    max_connection_count=int(
                        product['advertisedMaxClientCount']),
                    max_partition_count=int(
                        product['advertisedMaxPartitionCount']))

        return None

    def scale_cluster(self, nodes_count):
        """Scale out/in cluster to specified number of nodes.

        Uses cloud admin api.
        """
        payload = {
            'cluster_id': self.cluster_id,
            'nodes_count': str(nodes_count)
        }
        return self.cloudv2._http_post(base_url=self.config.admin_api_url,
                                       endpoint='/ScaleCluster',
                                       json=payload)
