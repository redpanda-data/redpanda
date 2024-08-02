from typing import Any, Literal, overload
import requests
from typing import Union


class RpCloudApiClient(object):
    def __init__(self, config, log):
        self._config = config
        self._token = None
        self._logger = log
        self.lasterror = None

    def _handle_error(self, response: requests.Response, quite=False):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            self.lasterror = f'{e} {response.text}'
            if not quite:
                self._logger.error(self.lasterror)
            raise e
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

    @overload
    def _http_get(self,
                  endpoint: str = ...,
                  base_url=...,
                  override_headers=...,
                  *,
                  text_response: Literal[True],
                  quite: bool = ...,
                  **kwargs) -> str:
        ...

    @overload
    def _http_get(self,
                  endpoint: str = ...,
                  base_url=...,
                  override_headers=...,
                  text_response: Literal[False] = False,
                  quite: bool = ...,
                  **kwargs) -> Any:
        ...

    def _http_get(self,
                  endpoint='',
                  base_url=None,
                  override_headers={},
                  text_response=False,
                  quite=False,
                  **kwargs) -> Union[None, dict, str]:
        token = self._get_token()
        headers = override_headers or {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
        _base = base_url if base_url else self._config.api_url
        resp = requests.get(f'{_base}{endpoint}', headers=headers, **kwargs)
        _r = self._handle_error(resp, quite=quite)
        if text_response:
            return _r.text
        else:
            return _r.json()

    def _http_post(self,
                   base_url=None,
                   endpoint='',
                   override_headers={},
                   **kwargs):
        token = self._get_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        } | override_headers
        _base = base_url if base_url else self._config.api_url
        resp = requests.post(f'{_base}{endpoint}', headers=headers, **kwargs)
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

    @staticmethod
    def namespace_endpoint(uuid=None):
        _e = "/api/v1/namespaces"
        if uuid:
            _e += f"/{uuid}"
        return _e

    @staticmethod
    def cluster_endpoint(id=None):
        _e = "/api/v1/clusters"
        if id:
            _e += f"/{id}"
        return _e

    @staticmethod
    def network_endpoint(id=None):
        _e = "/api/v1/networks"
        if id:
            _e += f"/{id}"
        return _e

    @staticmethod
    def network_peering_endpoint(id=None, peering_id=None):
        _e = "/api/v1/networks"
        if id:
            _e += f"/{id}/network-peerings"
            if peering_id:
                _e += f"/{peering_id}"
        return _e

    def _prepare_params(self, ns_uuid=None):
        params = {}
        if ns_uuid:
            params['namespaceUuid'] = ns_uuid
        return params

    def list_namespaces(self, include_deleted=False):
        # Use local var to manupulate output
        _ret = self._http_get(self.namespace_endpoint())
        # Filter out deleted ones
        if include_deleted:
            _namespaces = _ret
        else:
            _namespaces = [n for n in _ret if not n['deleted']]
        # return it
        return _namespaces

    def list_networks(self, ns_uuid=None):
        # get networks for a namespace
        _ret = self._http_get(self.network_endpoint(),
                              params=self._prepare_params(ns_uuid))
        # return it
        return _ret

    def list_clusters(self, ns_uuid=None):
        # get networks for a namespace
        _ret = self._http_get(self.cluster_endpoint(),
                              params=self._prepare_params(ns_uuid))
        # return it
        return _ret

    def list_network_peerings(self, network_id, ns_uuid=None):
        _ret = self._http_get(self.network_peering_endpoint(id=network_id),
                              params=self._prepare_params(ns_uuid=ns_uuid))
        return _ret

    def get_network(self, network_id):
        _network = self._http_get(self.network_endpoint(id=network_id))
        return _network

    def get_resource(self, resource_handle) -> Union[None, dict, str]:
        _r = None
        try:
            _r = self._http_get(endpoint=resource_handle)
            self._logger.debug(
                f"...resource requested with '{resource_handle}'")
        except Exception as e:
            self._logger.warning(f"# Warning failed to get resource: {e}")
        return _r

    def delete_namespace(self, uuid):
        _r = self._http_delete(endpoint=self.namespace_endpoint(uuid=uuid))
        # Check status
        return _r

    def delete_resource(self, resource_handle):
        _r = None
        try:
            _r = self._http_delete(endpoint=resource_handle)
            self._logger.debug(f"...delete requested for '{resource_handle}'")
        except Exception as e:
            self._logger.warning(f"# Warning deletion failed: {e}")
            return False
        return _r
