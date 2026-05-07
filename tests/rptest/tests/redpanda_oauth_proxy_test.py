# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.keycloak import KC_HTTPS_PORT
from rptest.services.mitmproxy import MitmproxyService
from rptest.tests.cluster_config_test import wait_for_version_sync
from rptest.tests.redpanda_oauth_test import (
    CLIENT_ID,
    EXAMPLE_TOPIC,
    RedpandaOIDCTestBase,
)
from rptest.util import firewall_blocked


class OIDCViaProxyTestBase(RedpandaOIDCTestBase):
    """Common setup and helpers for the OIDC-via-proxy variants.

    An iptables DROP rule blocks direct egress from each Redpanda
    node to Keycloak's HTTPS port (via firewall_blocked), so the
    only working path to the IdP is via mitmproxy.
    """

    # If True, oidc_http_proxy_url is wired into extra_rp_conf before
    # broker start. Override to False for variants that PATCH the URL
    # at runtime (e.g. live-reload).
    SET_PROXY_URL_AT_STARTUP = True

    def __init__(self, test_context, **kwargs):
        super().__init__(test_context, use_ssl=True, **kwargs)
        self.mitmproxy = MitmproxyService(test_context)

    def _prepare_mitmproxy(self):
        """Hook for subclasses to enable a TLS listener before start."""
        pass

    def setUp(self):
        # Skips super().setUp(): broker start is deferred to
        # _start_broker() so it runs inside firewall_blocked.
        self._prepare_mitmproxy()
        self.mitmproxy.start()
        if self.SET_PROXY_URL_AT_STARTUP:
            self.redpanda.add_extra_rp_conf(
                {"oidc_http_proxy_url": self.mitmproxy.proxy_url()}
            )

    def _start_broker(self):
        # Goes directly to RedpandaOIDCTestBase to skip the immediate
        # parent's setUp (which only does mitmproxy bookkeeping).
        RedpandaOIDCTestBase.setUp(self)

    def tearDown(self):
        try:
            super().tearDown()
        finally:
            self.mitmproxy.safe_stop()

    @property
    def _keycloak_host(self) -> str:
        return self.keycloak.host(self.keycloak.nodes[0])

    def _setup_oauth_principal_and_topic(self):
        kc_node = self.keycloak.nodes[0]
        service_user_id = self.create_service_user(CLIENT_ID)
        self.rpk.create_topic(EXAMPLE_TOPIC)
        self.rpk.sasl_allow_principal(
            f"User:{service_user_id}",
            ["all"],
            "topic",
            EXAMPLE_TOPIC,
            self.su_username,
            self.su_password,
            self.su_algorithm,
        )
        cfg = self.keycloak.generate_oauth_config(kc_node, CLIENT_ID)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        return cfg

    def _make_oauth_producer(self, cfg):
        return PythonLibrdkafka(
            self.redpanda,
            algorithm="OAUTHBEARER",
            oauth_config=cfg,
            tls_cert=self.client_cert,
        ).get_producer()

    def _wait_for_oauth_auth(self, producer, *, err_msg):
        # list_topics requires the broker to have validated the
        # bearer token, which requires JWKS - so a successful list
        # confirms the OIDC fetch path completed.
        producer.poll(0.0)
        wait_until(
            lambda: set(producer.list_topics(timeout=5).topics.keys())
            == {EXAMPLE_TOPIC},
            timeout_sec=30,
            err_msg=err_msg,
        )

    def _run_oidc_discovery_via_proxy(self):
        with firewall_blocked(self.redpanda.nodes, KC_HTTPS_PORT):
            self._start_broker()
            cfg = self._setup_oauth_principal_and_topic()
            producer = self._make_oauth_producer(cfg)
            self._wait_for_oauth_auth(
                producer,
                err_msg="OAUTHBEARER-authenticated client could not list topics",
            )

            producer.produce(topic=EXAMPLE_TOPIC, key="k", value="v")
            producer.flush(10)

            # Confirms mitmproxy actually saw Redpanda's CONNECT to
            # the Keycloak hostname, ruling out the degenerate pass
            # where Redpanda somehow bypassed the DROP rule.
            self.mitmproxy.assert_proxied_host(self._keycloak_host)


class OIDCViaProxyTest(OIDCViaProxyTestBase):
    """Plaintext http:// proxy variant: mitmproxy's listener is
    unwrapped, Redpanda TCP-connects directly and issues CONNECT.
    """

    @cluster(num_nodes=5)
    def test_oidc_discovery_via_http_proxy(self):
        self._run_oidc_discovery_via_proxy()


class OIDCViaHttpsProxyTest(OIDCViaProxyTestBase):
    """TLS https:// proxy variant (nested TLS): Redpanda
    TLS-handshakes with mitmproxy's listener, sends CONNECT through
    the wrapped socket, then TLS-handshakes with Keycloak inside the
    tunnel.

    mitmproxy's regular-mode listener autodetects plaintext HTTP vs
    TLS and presents the cert configured via --certs. That cert is
    signed by the same TLSCertManager CA already installed into each
    broker's system trust by RedpandaOIDCTestBase when use_ssl=True,
    so Redpanda's OIDC system-trust credentials validate it.
    """

    def _prepare_mitmproxy(self):
        # self.tls is populated by RedpandaOIDCTestBase when use_ssl=True,
        # which the parent class always passes.
        assert self.tls is not None
        hostname = self.mitmproxy.node.account.hostname
        cert = self.tls.create_cert(
            hostname, common_name=hostname, name="mitmproxy-server"
        )
        self.mitmproxy.set_server_cert(cert)

    @cluster(num_nodes=5)
    def test_oidc_discovery_via_http_proxy(self):
        self._run_oidc_discovery_via_proxy()


class OIDCProxyLiveReloadTest(OIDCViaProxyTestBase):
    """Asserts that toggling oidc_http_proxy_url at runtime re-routes
    subsequent OIDC fetches via the watch() hookup in oidc_service.
    Without that binding, a PATCHed URL would only take effect on
    broker restart.
    """

    SET_PROXY_URL_AT_STARTUP = False

    # Pre-PATCH discovery + JWKS fetches error out because direct
    # egress is blocked and no proxy is configured yet.
    EXPECTED_ERROR_LOGS = [
        "Error updating metadata",
        "Error updating jwks",
    ]

    @cluster(num_nodes=5, log_allow_list=EXPECTED_ERROR_LOGS)
    def test_proxy_url_live_reload(self):
        with firewall_blocked(self.redpanda.nodes, KC_HTTPS_PORT):
            self._start_broker()
            cfg = self._setup_oauth_principal_and_topic()

            # Live-reload the proxy URL. The watch() hookup must spawn
            # a fresh update() so the next discovery + JWKS refresh
            # routes through mitmproxy and succeeds.
            admin = Admin(self.redpanda)
            result = admin.patch_cluster_config(
                upsert={"oidc_http_proxy_url": self.mitmproxy.proxy_url()}
            )
            wait_for_version_sync(admin, self.redpanda, result["config_version"])

            producer = self._make_oauth_producer(cfg)
            self._wait_for_oauth_auth(
                producer,
                err_msg=(
                    "OAUTHBEARER auth did not succeed after PATCHing "
                    "oidc_http_proxy_url; the watch() hookup likely "
                    "failed to re-trigger the discovery + JWKS fetch."
                ),
            )

            # Confirms the post-PATCH OIDC fetch actually reached
            # Keycloak through mitmproxy.
            self.mitmproxy.assert_proxied_host(self._keycloak_host)


class OIDCProxyRejectsPlaintextOriginTest(RedpandaOIDCTestBase):
    """Asserts the runtime rejection of `oidc_http_proxy_url` set with a
    plaintext `oidc_discovery_url`: CONNECT tunnelling only works for
    TLS origins (plaintext origins would require absolute-form HTTP
    request rewriting, RFC 9112 §3.2.2, which is not implemented), so
    the broker must produce a named operator-facing error rather than
    silently bypassing the proxy or timing out.
    """

    # Load-bearing substring of the operator-facing rejection message
    # at src/v/security/oidc_service.cc make_request().
    REJECTION_LOG = (
        "oidc_http_proxy_url is set but the OIDC endpoint scheme is not https"
    )

    # Errors that necessarily accompany the rejection. The discovery
    # refresh raises first; the JWKS refresh then fails because
    # jwks_uri is never populated. Both are expected in this test.
    EXPECTED_ERROR_LOGS = [
        REJECTION_LOG,
        "Error updating jwks: .* jwks_uri is not set",
    ]

    def __init__(self, test_context, **kwargs):
        super().__init__(test_context, use_ssl=False, **kwargs)
        self.mitmproxy = MitmproxyService(test_context)

    def setUp(self):
        # mitmproxy provides a reachable URL for the per-property validator.
        self.mitmproxy.start()
        self.redpanda.add_extra_rp_conf(
            {"oidc_http_proxy_url": self.mitmproxy.proxy_url()}
        )
        try:
            super().setUp()
        except Exception:
            self.mitmproxy.safe_stop()
            raise

    def tearDown(self):
        self.mitmproxy.safe_stop()
        super().tearDown()

    @cluster(num_nodes=5, log_allow_list=EXPECTED_ERROR_LOGS)
    def test_rejects_plaintext_origin(self):
        wait_until(
            lambda: self.redpanda.search_log_any(self.REJECTION_LOG),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=(f"expected rejection log line not found: {self.REJECTION_LOG!r}"),
        )


class OIDCProxyRejectedAtConfigCommitTest(RedpandaOIDCTestBase):
    """Asserts the commit-time rejection of oidc_http_proxy_url set with a
    plaintext oidc_discovery_url: a cluster-config PATCH that leaves
    the two in that combination is rejected with HTTP 400 before the
    change is replicated via raft. Complements the runtime-rejection
    coverage in OIDCProxyRejectsPlaintextOriginTest.

    Covers both directions the bad combination can arrive through:
      - setting oidc_http_proxy_url while oidc_discovery_url is http;
      - changing oidc_discovery_url to http while oidc_http_proxy_url is
        already set.

    The validator is constructed to check the final post-PATCH state
    and is therefore symmetric by design; testing both directions
    guards against a regression where only one field's write path
    triggers the check.
    """

    # Unreachable proxy URL → OIDC refresh logs ERROR. Not the
    # behaviour under test; whitelist rather than contrive a
    # reachable dummy proxy.
    EXPECTED_ERROR_LOGS = [
        "Error updating metadata",
        "Error updating jwks",
    ]

    def __init__(self, test_context, **kwargs):
        # use_ssl=False so the discovery URL starts as http://.
        super().__init__(test_context, use_ssl=False, **kwargs)

    def _assert_rejected(self, admin: Admin, upsert: dict) -> None:
        try:
            admin.patch_cluster_config(upsert=upsert)
        except HTTPError as e:
            if e.response.status_code != 400:
                raise
            errors = e.response.json()
            assert "oidc_http_proxy_url" in errors, (
                f"expected 'oidc_http_proxy_url' in error keys, got {errors}"
            )
            assert "https" in errors["oidc_http_proxy_url"], (
                f"expected message to mention https, got {errors['oidc_http_proxy_url']!r}"
            )
        else:
            raise RuntimeError(
                f"expected HTTP 400 for upsert={upsert}, but PATCH succeeded"
            )

    @cluster(num_nodes=4)
    def test_rejects_setting_proxy_while_discovery_http(self):
        # Direction 1: discovery stays http, PATCH adds oidc_http_proxy_url.
        admin = Admin(self.redpanda)
        self._assert_rejected(
            admin, upsert={"oidc_http_proxy_url": "http://proxy.example:8888"}
        )

    @cluster(num_nodes=4, log_allow_list=EXPECTED_ERROR_LOGS)
    def test_rejects_reverting_discovery_to_http_while_proxy_set(self):
        # Direction 2: reach a valid (proxy set, https discovery) state
        # via admin PATCH, then try to revert discovery to http.
        # wait_for_version_sync between PATCHes: the cross-field
        # validator reads shard_local_cfg at request time, so each
        # intermediate config must propagate before the next PATCH runs
        # or the validator sees a stale value and rejects prematurely.
        admin = Admin(self.redpanda)
        result = admin.patch_cluster_config(
            upsert={
                "oidc_discovery_url": (
                    "https://idp.example:8443/.well-known/openid-configuration"
                )
            }
        )
        wait_for_version_sync(admin, self.redpanda, result["config_version"])
        result = admin.patch_cluster_config(
            upsert={"oidc_http_proxy_url": "http://proxy.example:8888"}
        )
        wait_for_version_sync(admin, self.redpanda, result["config_version"])
        self._assert_rejected(
            admin,
            upsert={
                "oidc_discovery_url": (
                    "http://idp.example:8080/.well-known/openid-configuration"
                )
            },
        )
