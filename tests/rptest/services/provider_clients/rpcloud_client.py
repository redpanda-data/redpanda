from typing import Any, Literal, Union, overload

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RpCloudApiClient(object):
    def __init__(self, config, log):
        self._config = config
        self._token = None
        self._logger = log
        self.lasterror = None
        self._session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a session with retry for transient connection errors."""
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=[
                requests.codes.bad_gateway,
                requests.codes.service_unavailable,
                requests.codes.gateway_timeout,
            ],
            allowed_methods=["GET", "POST", "PATCH", "DELETE", "HEAD"],
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        return session

    def _handle_error(self, response: requests.Response, quite=False):
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            self.lasterror = f"{e} {response.text}"
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
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            data = {
                "grant_type": "client_credentials",
                "client_id": f"{self._config.oauth_client_id}",
                "client_secret": f"{self._config.oauth_client_secret}",
                "audience": f"{self._config.oauth_audience}",
            }
            resp = self._session.post(
                f"{self._config.oauth_url}", headers=headers, data=data
            )
            _r = self._handle_error(resp)
            if _r is None:
                return _r
            j = resp.json()
            self._token = j["access_token"]
        return self._token

    def _with_token_refresh(self, send, *, refresh_eligible):
        """Issue a request via `send()` and, on a 401 while using our cached
        bearer, drop the token, refresh, and retry once.

        The cloud-api bearer is cached for the cluster's lifetime; long-running
        tests can outlive its TTL or hit a server-side rotation, after which
        every call 401s. DEVPROD-4327.
        """
        resp = send()
        if (
            refresh_eligible
            and resp.status_code == requests.codes.unauthorized
            and self._token is not None
        ):
            self._logger.warning(
                "cloud-api returned 401; refreshing OAuth token and retrying once"
            )
            self._token = None
            resp = send()
        return resp

    @overload
    def _http_get(
        self,
        endpoint: str = ...,
        base_url=...,
        override_headers=...,
        *,
        text_response: Literal[True],
        quite: bool = ...,
        **kwargs,
    ) -> str: ...

    @overload
    def _http_get(
        self,
        endpoint: str = ...,
        base_url=...,
        override_headers=...,
        text_response: Literal[False] = False,
        quite: bool = ...,
        **kwargs,
    ) -> Any: ...

    def _http_get(
        self,
        endpoint="",
        base_url=None,
        override_headers=None,
        text_response=False,
        quite=False,
        **kwargs,
    ) -> Union[None, dict, str]:
        _base = base_url if base_url else self._config.api_url

        def send():
            if override_headers:
                headers = override_headers
            else:
                token = self._get_token()
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                }
            return self._session.get(f"{_base}{endpoint}", headers=headers, **kwargs)

        resp = self._with_token_refresh(send, refresh_eligible=not override_headers)
        _r = self._handle_error(resp, quite=quite)
        if text_response:
            return _r.text
        else:
            return _r.json()

    def _http_post(self, base_url=None, endpoint="", override_headers=None, **kwargs):
        _base = base_url if base_url else self._config.api_url

        def send():
            token = self._get_token()
            self._logger.debug(f"acquired cloud-api token ({len(token)} chars)")
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            } | (override_headers or {})
            return self._session.post(f"{_base}{endpoint}", headers=headers, **kwargs)

        resp = self._with_token_refresh(send, refresh_eligible=True)
        _r = self._handle_error(resp)
        return _r if _r is None else _r.json()

    def _http_patch(
        self,
        base_url: str | None = None,
        endpoint: str = "",
        override_headers: dict[str, str] | None = None,
        **kwargs,
    ):
        """
        Like _http_post but uses PATCH.
        Injects Bearer token and Accept header the same way.
        """
        _base = base_url if base_url else self._config.api_url

        def send():
            token = self._get_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            } | (override_headers or {})
            return self._session.patch(f"{_base}{endpoint}", headers=headers, **kwargs)

        resp = self._with_token_refresh(send, refresh_eligible=True)
        _r = self._handle_error(resp)
        return _r if _r is None else _r.json()

    def _http_delete(self, base_url=None, endpoint="", **kwargs):
        _base = base_url if base_url else self._config.api_url

        def send():
            token = self._get_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
            return self._session.delete(f"{_base}{endpoint}", headers=headers, **kwargs)

        resp = self._with_token_refresh(send, refresh_eligible=True)
        _r = self._handle_error(resp)
        return _r if _r is None else _r.json()

    @staticmethod
    def namespace_endpoint(uuid=None):
        _e = "/v1/resource-groups"
        if uuid:
            _e += f"/{uuid}"
        return _e

    @staticmethod
    def cluster_endpoint(id=None):
        _e = "/v1/clusters"
        if id:
            _e += f"/{id}"
        return _e

    @staticmethod
    def serverless_cluster_endpoint(id=None):
        _e = "/v1/serverless/clusters"
        if id:
            _e += f"/{id}"
        return _e

    # TODO: DEVPROD-2525 - keep legacy endpoint for install pack version
    # and prometheus credentials which have no public-api v1 equivalent
    @staticmethod
    def legacy_cluster_endpoint(id=None):
        _e = "/api/v1/clusters"
        if id:
            _e += f"/{id}"
        return _e

    @staticmethod
    def network_endpoint(id=None):
        _e = "/v1/networks"
        if id:
            _e += f"/{id}"
        return _e

    @staticmethod
    def network_peering_endpoint(id=None, peering_id=None):
        _e = "/v1/networks"
        if id:
            _e += f"/{id}/network-peerings"
            if peering_id:
                _e += f"/{peering_id}"
        return _e

    def _prepare_params(self, ns_uuid=None):
        params = {}
        if ns_uuid:
            params["namespaceUuid"] = ns_uuid
        return params

    def list_clusters(self, ns_uuid=None):
        # get clusters for a namespace
        _ret = self._http_get(
            self.cluster_endpoint(),
            base_url=self._config.public_api_url,
            params=self._prepare_params(ns_uuid),
        )
        # return it
        return _ret["clusters"]

    def list_namespaces(self):
        return self._http_get(
            self.namespace_endpoint(), base_url=self._config.public_api_url
        )["resource_groups"]

    def list_networks(self, ns_uuid=None):
        _ret = self._http_get(
            self.network_endpoint(),
            base_url=self._config.public_api_url,
            params=self._prepare_params(ns_uuid),
        )
        return _ret.get("networks", [])

    def list_network_peerings(self, network_id, ns_uuid=None):
        _ret = self._http_get(
            self.network_peering_endpoint(id=network_id),
            base_url=self._config.public_api_url,
            params=self._prepare_params(ns_uuid=ns_uuid),
        )
        return _ret.get("network_peerings", [])

    def get_cluster(self, cluster_id: str):
        _cluster = self._http_get(
            self.cluster_endpoint(id=cluster_id), base_url=self._config.public_api_url
        )
        return _cluster["cluster"]

    def get_serverless_cluster(self, cluster_id: str):
        _cluster = self._http_get(
            self.serverless_cluster_endpoint(id=cluster_id),
            base_url=self._config.public_api_url,
        )
        return _cluster["serverless_cluster"]

    # TODO: DEVPROD-2525 - keep for install pack version and prometheus
    # credentials which have no public-api v1 equivalent
    def get_legacy_cluster(self, cluster_id: str):
        _cluster = self._http_get(self.legacy_cluster_endpoint(id=cluster_id))
        return _cluster

    def get_network(self, network_id):
        _network = self._http_get(
            self.network_endpoint(id=network_id),
            base_url=self._config.public_api_url,
        )
        return _network.get("network", _network)

    def get_resource(self, resource_handle) -> Union[None, dict, str]:
        _r = None
        base = self._config.public_api_url if resource_handle.startswith("/v1/") else None
        try:
            _r = self._http_get(endpoint=resource_handle, base_url=base)
            self._logger.debug(f"...resource requested with '{resource_handle}'")
        except Exception as e:
            self._logger.warning(f"# Warning failed to get resource: {e}")
        return _r

    def delete_namespace(self, uuid):
        _r = self._http_delete(
            endpoint=self.namespace_endpoint(uuid=uuid),
            base_url=self._config.public_api_url,
        )
        # Check status
        return _r

    def delete_resource(self, resource_handle):
        _r = None
        base = self._config.public_api_url if resource_handle.startswith("/v1/") else None
        try:
            _r = self._http_delete(endpoint=resource_handle, base_url=base)
            self._logger.debug(f"...delete requested for '{resource_handle}'")
        except Exception as e:
            self._logger.warning(f"# Warning deletion failed: {e}")
            return False
        return _r
