import collections
import json
import os
import requests
import uuid

from dataclasses import dataclass
from typing import Optional

from ducktape.utils.util import wait_until

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
    Configuration for the Cloud Cluster.
    Should be the same as in context.globals['cloud_cluster']
    Otherwise there will be error for not supplied parameters
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


class CloudCluster():
    """
    Operations on a Redpanda Cloud cluster via the swagger API.

    Creates and deletes a cluster. Will also create a new namespace for
    that cluster.
    """

    CHECK_TIMEOUT_SEC = 3600
    CHECK_BACKOFF_SEC = 60.0

    def __init__(self, logger, cluster_config, delete_namespace=False):
        """
        Initializes the object, but does not create clusters. Use
        `create` method to create a cluster.


        :param logger: logging object
        :param oauth_url: full url of the oauth endpoint to get a token
        :param oauth_client_id: client id from redpanda cloud ui page for clients
        :param oauth_client_secret: client secret from redpanda cloud ui page for clients
        :param oauth_audience: oauth audience
        :param api_url: url of hostname for swagger api without trailing slash char
        :param cluster_id: if not empty string, will skip creating a new cluster
        :param delete_namespace: if False, will skip namespace deletion step after tests are run
        """

        self._logger = logger
        self._delete_namespace = delete_namespace
        # JSON is serialized directly to dataclass
        self.config = CloudClusterConfig(**cluster_config)
        # ensure that variables are in uppercase
        self.config.type = self.config.type.upper()
        self.config.provider = self.config.provider.upper()
        # Init API client
        self.cloudv2 = RpCloudApiClient(self.config, logger)

        # unique 8-char identifier to be used when creating names of things for this cluster
        self._unique_id = str(uuid.uuid1())[:8]

    @property
    def cluster_id(self):
        """
        The clusterId of the created cluster.
        """

        return self._cluster_id

    def _create_namespace(self):
        name = f'rp-ducktape-ns-{self._unique_id}'  # e.g. rp-ducktape-ns-3b36f516
        self._logger.debug(f'creating namespace name {name}')
        body = {'name': name}
        r = self.cloudv2._http_post(endpoint='/api/v1/namespaces', json=body)
        self._logger.debug(f'created namespaceUuid {r["id"]}')
        # save namespace name
        self.config.namespace = name
        return r['id']

    def _cluster_ready(self, namespace_uuid, name):
        self._logger.debug(f'checking readiness of cluster {name}')
        params = {'namespaceUuid': namespace_uuid}
        clusters = self.cloudv2._http_get(endpoint='/api/v1/clusters',
                                          params=params)
        for c in clusters:
            if c['name'] == name:
                if c['state'] == 'ready':
                    return True
        return False

    def _get_cluster_id_and_network_id(self, namespace_uuid, name):
        """
        Get clusterId.

        :param namespace_uuid: namespaceUuid the cluster is contained in
        :param name: name of the cluster
        :return: clusterId as a string or None if not found
        """

        params = {'namespaceUuid': namespace_uuid}
        clusters = self.cloudv2._http_get(endpoint='/api/v1/clusters',
                                          params=params)
        for c in clusters:
            if c['name'] == name:
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

    def _get_region_id(self, cluster_type, provider, region):
        """Get the region id for a region.

        :param cluster_type: cluster type, e.g. 'FMC'
        :param provider: cloud provider, e.g. 'AWS'
        :param region: region name, e.g. 'us-west-2'
        :return: id, e.g. 'cckac9vvbr5ofm048jjg'
        """

        params = {'cluster_type': cluster_type}
        regions = self.cloudv2._http_get(
            endpoint='/api/v1/clusters-resources/regions', params=params)
        for r in regions[provider]:
            if r['name'] == region:
                return r['id']
        return None

    def _get_product_id(self,
                        config_profile_name,
                        provider,
                        cluster_type=None,
                        region=None,
                        install_pack_ver=None):
        """Get the product id for the first matching config profile name using filter parameters.

        :param config_profile_name: config profile name, e.g. 'tier-1-aws'
        :param provider: cloud provider filter, e.g. 'AWS'
        :param cluster_type: cluster type filter, e.g. 'FMC'
        :param region: region name filter, e.g. 'us-west-2'
        :param install_pack_ver: install pack version filter, e.g. '23.2.20230707135118'
        :return: productId, e.g. 'chqrd4q37efgkmohsbdg'
        """

        params = {
            'cloud_provider': provider,
            'cluster_type': cluster_type,
            'region': region,
            'install_pack_version': install_pack_ver
        }
        products = self.cloudv2._http_get(
            endpoint='/api/v1/clusters-resources/products', params=params)
        for p in products:
            if p['redpandaConfigProfileName'] == config_profile_name:
                return p['id']
        return None

    def create(self,
               config_profile_name: str = 'tier-1-aws',
               superuser: Optional[SaslCredentials] = None) -> str:
        """Create a cloud cluster and a new namespace; block until cluster is finished creating.

        :param config_profile_name: config profile name, default 'tier-1-aws'
        :return: clusterId, e.g. 'cimuhgmdcaa1uc1jtabc'
        """

        if self.config.id != '':
            self._logger.warn(
                f'will not create cluster; already have cluster_id {self.config.id}'
            )
            return self.config.id

        namespace_uuid = self._create_namespace()
        name = f'rp-ducktape-cluster-{self._unique_id}'  # e.g. rp-ducktape-cluster-3b36f516
        install_pack_ver = self._get_install_pack_ver()
        # 'FMC'
        cluster_type = self.config.type
        # 'AWS'
        provider = self.config.provider
        # 'us-west-2'
        region = self.config.region
        region_id = self._get_region_id(cluster_type, provider, region)
        zones = ['usw2-az1']
        product_id = self._get_product_id(config_profile_name, provider,
                                          cluster_type, region,
                                          install_pack_ver)
        public = True  # TODO get value from globals config setting
        if public:
            connection_type = 'public'
        else:
            connection_type = 'private'

        self._logger.info(f'creating cluster name {name}')
        body = {
            "cluster": {
                "name": name,
                "productId": product_id,
                "spec": {
                    "clusterType": cluster_type,
                    "connectors": {
                        "enabled": True
                    },
                    "installPackVersion": install_pack_ver,
                    "isMultiAz": False,
                    "networkId": "",
                    "provider": provider,
                    "region": region,
                    "zones": zones,
                }
            },
            "connectionType": connection_type,
            "namespaceUuid": namespace_uuid,
            "network": {
                "displayName": f"{connection_type}-network-{name}",
                "spec": {
                    "cidr": "10.1.0.0/16",
                    "deploymentType": cluster_type,
                    "installPackVersion": install_pack_ver,
                    "provider": provider,
                    "regionId": region_id,
                }
            },
        }

        self._logger.debug(f'body: {json.dumps(body)}')

        r = self.cloudv2._http_post(
            endpoint='/api/v1/workflows/network-cluster', json=body)

        self._logger.info(
            f'waiting for creation of cluster {name} namespaceUuid {r["namespaceUuid"]}, checking every {self.CHECK_BACKOFF_SEC} seconds'
        )

        wait_until(
            lambda: self._cluster_ready(namespace_uuid, name),
            timeout_sec=self.CHECK_TIMEOUT_SEC,
            backoff_sec=self.CHECK_BACKOFF_SEC,
            err_msg=f'Unable to deterimine readiness of cloud cluster {name}')
        self.config.id, network_id = self._get_cluster_id(namespace_uuid, name)

        if superuser is not None:
            self._logger.debug(
                f'super username: {superuser.username}, algorithm: {superuser.algorithm}'
            )
            self._create_user(superuser)
            self._create_acls(superuser.username)

        if not public:
            # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-categories.html
            # curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/$(ip -br link show eth0 | awk '{print$3}')/owner-id
            # curl -s http://169.254.169.254/latest/meta-data/network/interfaces/macs/$(ip -br link show eth0 | awk '{print$3}')/vpc-id
            network_id = ''  # TODO get network ID from newly-created cluster
            body = {
                "networkPeering": {
                    "displayName": f'peer-{name}',
                    "spec": {
                        "provider": "AWS",
                        "cloudProvider": {
                            "aws": {
                                "peerOwnerId": self.config.peer_owner_id,
                                "peerVpcId": self.config.peer_vpc_id
                            }
                        }
                    }
                },
                "namespaceUuid": namespace_uuid
            }
            resp = self.cloudv2._http_post(
                f'/api/v1/networks/{network_id}/network-peerings', json=body)
            # TODO accept the AWS VPC peering request
            # TODO create route between vpc and peering connection

        return self._cluster_id

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
