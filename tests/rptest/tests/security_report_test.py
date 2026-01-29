# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass, fields

from ducktape.mark import matrix, parametrize

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    RedpandaService,
    SaslCredentials,
    SchemaRegistryConfig,
    SecurityConfig,
)
from rptest.services.tls import TLSCertManager
from rptest.tests.pandaproxy_test import PandaProxyMTLSBase
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.schema_registry_test import SchemaRegistryMTLSBase
from rptest.tests.scram_test import SaslPlainTLSProvider
from rptest.tests.audit_log_test import (
    AuditLogConfig,
    AuditLogTestBase,
    AuditLogTestSecurityConfig,
)
from rptest.utils.mode_checks import skip_fips_mode


def make_from_dict(class_name, values):
    fields_set = {f.name for f in fields(class_name) if f.init}
    # Drop all values not in class_name
    filtered = {k: v for k, v in values.items() if k in fields_set}
    # Set all fields that don't have a value to None
    field_values = {k: filtered[k] if k in filtered else None for k in fields_set}
    return class_name(**field_values)


KAFKA_INTERFACE_KEYS = [
    "name",
    "host",
    "port",
    "advertised_host",
    "advertised_port",
    "tls_enabled",
    "mutual_tls_enabled",
    "authentication_method",
    "authorization_enabled",
]

sasl_default_mechs = ["SCRAM"]
sasl_plain_mechs = ["SCRAM", "PLAIN"]


@dataclass
class KafkaInterface:
    tls_enabled: bool
    mutual_tls_enabled: bool
    authorization_enabled: bool
    authentication_method: str
    supported_sasl_mechanisms: list | None

    @staticmethod
    def expected_keys() -> list[str]:
        return KAFKA_INTERFACE_KEYS

    @staticmethod
    def default():
        return KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=False,
            authentication_method="None",
            supported_sasl_mechanisms=None,
        )


RPC_INTERFACE_KEYS = [
    "host",
    "port",
    "advertised_host",
    "advertised_port",
    "tls_enabled",
    "mutual_tls_enabled",
]


@dataclass
class RpcInterface:
    tls_enabled: bool
    mutual_tls_enabled: bool

    @staticmethod
    def expected_keys() -> list[str]:
        return RPC_INTERFACE_KEYS

    @staticmethod
    def default():
        return RpcInterface(tls_enabled=False, mutual_tls_enabled=False)


ADMIN_INTERFACE_KEYS = [
    "name",
    "host",
    "port",
    "tls_enabled",
    "mutual_tls_enabled",
    "authentication_methods",
    "authorization_enabled",
]


@dataclass
class AdminInterface:
    tls_enabled: bool
    mutual_tls_enabled: bool
    authorization_enabled: bool
    authentication_methods: list[str]

    @staticmethod
    def expected_keys() -> list[str]:
        return ADMIN_INTERFACE_KEYS

    @staticmethod
    def default():
        return AdminInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=False,
            authentication_methods=[],
        )


PANDAPROXY_INTERFACE_KEYS = [
    "name",
    "host",
    "port",
    "advertised_host",
    "advertised_port",
    "tls_enabled",
    "mutual_tls_enabled",
    "authentication_methods",
    "authorization_enabled",
    "configured_authentication_method",
]


@dataclass
class PandaproxyInterface:
    tls_enabled: bool
    mutual_tls_enabled: bool
    authorization_enabled: bool
    authentication_methods: list[str]
    configured_authentication_method: str

    @staticmethod
    def expected_keys() -> list[str]:
        return PANDAPROXY_INTERFACE_KEYS

    @staticmethod
    def default():
        return PandaproxyInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=False,
            authentication_methods=[],
            configured_authentication_method="None",
        )


SCHEMA_REGISTRY_INTERFACE_KEYS = [
    "name",
    "host",
    "port",
    "tls_enabled",
    "mutual_tls_enabled",
    "authentication_methods",
    "authorization_enabled",
]


@dataclass
class SchemaRegistryInterface:
    tls_enabled: bool
    mutual_tls_enabled: bool
    authorization_enabled: bool
    authentication_methods: list[str]

    @staticmethod
    def expected_keys() -> list[str]:
        return SCHEMA_REGISTRY_INTERFACE_KEYS

    @staticmethod
    def default():
        return SchemaRegistryInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=False,
            authentication_methods=[],
        )


KAFKA_CLIENT_INTERFACE_KEYS = [
    "kafka_listener_name",
    "brokers",
    "tls_enabled",
    "mutual_tls_enabled",
    "configured_authentication_method",
]


@dataclass
class KafkaClientInterface:
    tls_enabled: bool
    mutual_tls_enabled: bool
    configured_authentication_method: str

    @staticmethod
    def expected_keys() -> list[str]:
        return KAFKA_CLIENT_INTERFACE_KEYS

    @staticmethod
    def default():
        return KafkaClientInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            configured_authentication_method="None",
        )


@dataclass
class SecurityAlert:
    affected_interface: str | None
    listener_name: str | None
    issue: str
    description: str


def validate_report(
    response,
    kafka_expected={},
    rpc_expected=RpcInterface.default(),
    admin_expected={},
    pandaproxy_expected=None,
    schema_registry_expected=None,
    schema_registry_client_expected=KafkaClientInterface.default(),
    audit_log_expected=None,
    expected_alerts=[],
    expected_missing_interfaces=[],
):
    assert response.status_code == 200, (
        f"Expected status code {200} but got {response.status_code}, instead.\n"
        f"Content: {response.content}"
    )

    def assert_key(key, data):
        assert key in data, f"Expected '{key}' key in '{data}'"

    def get_key(key, data):
        assert_key(key, data)
        return data[key]

    report_json = response.json()
    interfaces = get_key("interfaces", report_json)

    def assert_interface(json_data, expected_interface, interface_type):
        for key in interface_type.expected_keys():
            assert_key(key, json_data)

        interface = make_from_dict(interface_type, json_data)
        assert interface == expected_interface, (
            f"Generated interface doesn't match expected:\n"
            f"Report  : {interface}\n"
            f"Expected: {expected_interface}\n"
            f"In object: {json_data}\n"
        )

    for kafka_json in get_key("kafka", interfaces):
        name = kafka_json.get("name", "")
        expected_interface = kafka_expected.get(name, KafkaInterface.default())
        assert_interface(kafka_json, expected_interface, KafkaInterface)

    rpc_json = get_key("rpc", interfaces)
    assert_interface(rpc_json, rpc_expected, RpcInterface)

    for admin_json in get_key("admin", interfaces):
        name = admin_json.get("name", "")
        expected_interface = admin_expected.get(name, AdminInterface.default())
        assert_interface(admin_json, expected_interface, AdminInterface)

    if pandaproxy_expected is not None:
        for pp_json in get_key("pandaproxy", interfaces):
            name = pp_json.get("name", "")
            expected_interface = pandaproxy_expected.get(
                name, PandaproxyInterface.default()
            )
            assert_interface(pp_json, expected_interface, PandaproxyInterface)

    if schema_registry_expected is not None:
        for sr_json in get_key("schema_registry", interfaces):
            name = sr_json.get("name", "")
            expected_interface = schema_registry_expected.get(
                name, SchemaRegistryInterface.default()
            )
            assert_interface(sr_json, expected_interface, SchemaRegistryInterface)

        src_json = get_key("schema_registry_client", interfaces)
        assert_interface(
            src_json, schema_registry_client_expected, KafkaClientInterface
        )

    if audit_log_expected:
        alc_json = get_key("audit_log_client", interfaces)
        assert_interface(alc_json, audit_log_expected, KafkaClientInterface)

    alerts_json = get_key("alerts", report_json)
    alerts = [make_from_dict(SecurityAlert, a) for a in alerts_json]
    alerts_str = "\n".join(str(a) for a in alerts)

    for expected_alert in expected_alerts:
        assert expected_alert in alerts, (
            f"Expected alert:\n{expected_alert}\nnot found in alerts:\n{alerts_str}"
        )

    for interface in expected_missing_interfaces:
        assert interface not in interfaces, (
            f"Found interface '{interface}' in interfaces: {interfaces.keys()}"
        )


class NoSecurityReportTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            extra_rp_conf={
                "tls_min_version": "v1.0",
                "tls_enable_renegotiation": "true",
            },
            **kwargs,
        )

    def setUp(self):
        super().setUp()

    @cluster(num_nodes=3)
    def test_security_report(self):
        report = Admin(self.redpanda).security_report()

        expected_alerts = [
            SecurityAlert(
                affected_interface="kafka",
                listener_name="dnslistener",
                issue="NO_TLS",
                description='"kafka" interface "dnslistener" is not using TLS.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="kafka",
                listener_name="dnslistener",
                issue="NO_AUTHN",
                description='"kafka" interface "dnslistener" is not using authentication.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="kafka",
                listener_name="dnslistener",
                issue="NO_AUTHZ",
                description='"kafka" interface "dnslistener" is not using authorization.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="rpc",
                listener_name=None,
                issue="NO_TLS",
                description='"rpc" interface is not using TLS.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="admin",
                listener_name="iplistener",
                issue="NO_TLS",
                description='"admin" interface "iplistener" is not using TLS.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="admin",
                listener_name="iplistener",
                issue="NO_AUTHN",
                description='"admin" interface "iplistener" is not using authentication.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="admin",
                listener_name="iplistener",
                issue="NO_AUTHZ",
                description='"admin" interface "iplistener" is not using authorization.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface=None,
                listener_name=None,
                issue="INSECURE_MIN_TLS_VERSION",
                description="TLS minimum version is set to v1.0 which is less than v1.2."
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface=None,
                listener_name=None,
                issue="TLS_RENEGOTIATION",
                description="TLS renegotiation is enabled."
                " This is insecure and not recommended.",
            ),
        ]

        validate_report(
            report,
            expected_alerts=expected_alerts,
            expected_missing_interfaces=[
                "pandaproxy",
                "schema_registry",
                "schema_registry_client",
                "audit_log_client",
            ],
        )


class KafkaSecurityReportTest(RedpandaTest):
    def __init__(self, test_context):
        super(KafkaSecurityReportTest, self).__init__(
            test_context, num_brokers=3, extra_node_conf={"developer_mode": True}
        )
        self.tls = TLSCertManager(self.logger)

    def setUp(self):
        pass

    def _start_cluster(self, enable_tls: bool, authn: str):
        self.security = SecurityConfig()
        enable_sasl = authn == "SASL"
        self.security.enable_sasl = enable_sasl
        if enable_sasl:
            # Set to SASL/PLAIN to test alert for PLAIN
            self.security.sasl_mechanisms = ["SCRAM", "PLAIN"]
            # Set one listener to different values to verify override behavior
            self.security.sasl_mechanisms_overrides = [
                {"listener": "dnslistener", "sasl_mechanisms": ["SCRAM"]}
            ]
        if enable_tls:
            self.security.tls_provider = SaslPlainTLSProvider(tls=self.tls)
            if authn == "mTLS":
                self.security.endpoint_authn_method = "mtls_identity"
        self.redpanda.set_security_settings(self.security)
        super().setUp()

    @cluster(num_nodes=3)
    @parametrize(config={"enable_tls": False, "authn": "SASL"})
    @parametrize(config={"enable_tls": True, "authn": "SASL"})
    @parametrize(config={"enable_tls": True, "authn": "mTLS"})
    def test_security_report(self, config):
        enable_tls = config["enable_tls"]
        authn = config["authn"]
        self._start_cluster(enable_tls, authn)

        authz_enabled = authn == "SASL"
        maybe_sasl_plain_mechs = sasl_plain_mechs if authz_enabled else None
        maybe_sasl_default_mechs = sasl_default_mechs if authz_enabled else None

        dns_interface = KafkaInterface(
            tls_enabled=enable_tls,
            mutual_tls_enabled=enable_tls,
            authorization_enabled=authz_enabled,
            authentication_method=authn,
            supported_sasl_mechanisms=maybe_sasl_default_mechs,
        )
        ip_interface = KafkaInterface(
            tls_enabled=enable_tls,
            mutual_tls_enabled=enable_tls,
            authorization_enabled=authz_enabled,
            authentication_method=authn,
            supported_sasl_mechanisms=maybe_sasl_plain_mechs,
        )
        krb_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=authz_enabled,
            authentication_method=authn if authz_enabled else "None",
            supported_sasl_mechanisms=maybe_sasl_plain_mechs,
        )

        expected_alerts = []
        if authz_enabled:
            expected_alerts.extend(
                [
                    SecurityAlert(
                        affected_interface="kafka",
                        listener_name="iplistener",
                        issue="SASL_PLAIN",
                        description='"kafka" interface "iplistener" is using SASL/PLAIN.'
                        " This is insecure and not recommended.",
                    ),
                    SecurityAlert(
                        affected_interface="kafka",
                        listener_name="kerberoslistener",
                        issue="SASL_PLAIN",
                        description='"kafka" interface "kerberoslistener" is using SASL/PLAIN.'
                        " This is insecure and not recommended.",
                    ),
                ]
            )

        report = Admin(self.redpanda).security_report()
        validate_report(
            report,
            kafka_expected={
                "dnslistener": dns_interface,
                "iplistener": ip_interface,
                "kerberoslistener": krb_interface,
            },
            expected_alerts=expected_alerts,
        )


RPC_TLS_CONFIG = dict(
    enabled=True,
    require_client_auth=True,
    key_file=RedpandaService.TLS_SERVER_KEY_FILE,
    cert_file=RedpandaService.TLS_SERVER_CRT_FILE,
    truststore_file=RedpandaService.TLS_CA_CRT_FILE,
    crl_file=RedpandaService.TLS_CA_CRL_FILE,
)


class RpcTLSSecurityReportTest(RedpandaTest):
    def __init__(self, test_context):
        super().__init__(test_context)
        self.tls = TLSCertManager(self.logger)
        self.security = SecurityConfig()
        self.security.tls_provider = SaslPlainTLSProvider(tls=self.tls)
        self.redpanda.set_security_settings(self.security)

    def setUp(self):
        # Set up TLS for RPC
        cfg_overrides = {}

        def set_cfg(node):
            cfg_overrides[node] = dict(rpc_server_tls=RPC_TLS_CONFIG)

        self.redpanda.for_nodes(self.redpanda.nodes, set_cfg)

        self.redpanda.start(node_config_overrides=cfg_overrides)

    @cluster(num_nodes=3)
    def test_security_report(self):
        kafka_tls_interface = KafkaInterface(
            tls_enabled=True,
            mutual_tls_enabled=True,
            authorization_enabled=False,
            authentication_method="None",
            supported_sasl_mechanisms=None,
        )

        kafka_no_tls_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=False,
            authentication_method="None",
            supported_sasl_mechanisms=None,
        )

        rpc_inteface = RpcInterface(tls_enabled=True, mutual_tls_enabled=True)

        report = Admin(self.redpanda).security_report()
        validate_report(
            report,
            kafka_expected={
                "dnslistener": kafka_tls_interface,
                "iplistener": kafka_tls_interface,
                "kerberoslistener": kafka_no_tls_interface,
            },
            rpc_expected=rpc_inteface,
        )


ADMIN_TLS_CONFIG = dict(
    name="iplistener",
    enabled=True,
    require_client_auth=True,
    key_file=RedpandaService.TLS_SERVER_KEY_FILE,
    cert_file=RedpandaService.TLS_SERVER_CRT_FILE,
    truststore_file=RedpandaService.TLS_CA_CRT_FILE,
    crl_file=RedpandaService.TLS_CA_CRL_FILE,
)


class AdminSecurityReportTest(RedpandaTest):
    BOOTSTRAP_USERNAME = "bob"
    BOOTSTRAP_PASSWORD = "sekrit01234567"
    BOOTSTRAP_MECHANISM = "SCRAM-SHA-512"

    def __init__(self, test_context, *args, **kwargs):
        # Configure the cluster as a user might configure it for secure
        # bootstrap: i.e. all auth turned on from moment of creation.

        super().__init__(
            test_context,
            *args,
            environment={
                "RP_BOOTSTRAP_USER": f"{self.BOOTSTRAP_USERNAME}:{self.BOOTSTRAP_PASSWORD}:{self.BOOTSTRAP_MECHANISM}"
            },
            extra_rp_conf={"admin_api_require_auth": True, "superusers": ["bob"]},
            superuser=SaslCredentials(
                self.BOOTSTRAP_USERNAME,
                self.BOOTSTRAP_PASSWORD,
                self.BOOTSTRAP_MECHANISM,
            ),
            **kwargs,
        )
        self.tls = TLSCertManager(self.logger)
        self.security = SecurityConfig()
        self.security.http_authentication = ["BASIC", "OIDC"]
        self.security.tls_provider = SaslPlainTLSProvider(tls=self.tls)
        self.redpanda.set_security_settings(self.security)

    def setUp(self):
        # Set up TLS for Admin
        cfg_overrides = {}

        def set_cfg(node):
            cfg_overrides[node] = dict(admin_api_tls=ADMIN_TLS_CONFIG)

        self.redpanda.for_nodes(self.redpanda.nodes, set_cfg)

        self.redpanda.start(node_config_overrides=cfg_overrides)

    @cluster(num_nodes=3)
    def test_security_report(self):
        kafka_tls_interface = KafkaInterface(
            tls_enabled=True,
            mutual_tls_enabled=True,
            authorization_enabled=False,
            authentication_method="None",
            supported_sasl_mechanisms=None,
        )

        kafka_no_tls_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=False,
            authentication_method="None",
            supported_sasl_mechanisms=None,
        )

        with_tls_inteface = AdminInterface(
            tls_enabled=True,
            mutual_tls_enabled=True,
            authorization_enabled=True,
            authentication_methods=["BASIC", "OIDC"],
        )

        no_tls_interface = AdminInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=True,
            authentication_methods=["BASIC", "OIDC"],
        )

        admin = Admin(
            self.redpanda, auth=(self.BOOTSTRAP_USERNAME, self.BOOTSTRAP_PASSWORD)
        )
        report = admin.security_report()
        validate_report(
            report,
            kafka_expected={
                "dnslistener": kafka_tls_interface,
                "iplistener": kafka_tls_interface,
                "kerberoslistener": kafka_no_tls_interface,
            },
            admin_expected={
                "": no_tls_interface,
                "iplistener": with_tls_inteface,
            },
        )


class PandaproxyNoSecurityReportTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, pandaproxy_config=PandaproxyConfig(), **kwargs)

    def setUp(self):
        super().setUp()

    @cluster(num_nodes=3)
    def test_security_report(self):
        expected_alerts = [
            SecurityAlert(
                affected_interface="pandaproxy",
                listener_name="{{unnamed}}",
                issue="NO_TLS",
                description='"pandaproxy" interface "{{unnamed}}" is not using TLS.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="pandaproxy",
                listener_name="{{unnamed}}",
                issue="NO_AUTHN",
                description='"pandaproxy" interface "{{unnamed}}" is not using authentication.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="pandaproxy",
                listener_name="{{unnamed}}",
                issue="NO_AUTHZ",
                description='"pandaproxy" interface "{{unnamed}}" is not using authorization.'
                " This is insecure and not recommended.",
            ),
        ]

        report = Admin(self.redpanda).security_report()
        validate_report(report, pandaproxy_expected={}, expected_alerts=expected_alerts)


class PandaproxyMTLSSecurityReportTest(PandaProxyMTLSBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.setup_cluster()

    @cluster(num_nodes=3)
    def test_security_report(self):
        kafka_tls_interface = KafkaInterface(
            tls_enabled=True,
            mutual_tls_enabled=True,
            authorization_enabled=False,
            authentication_method="mTLS",
            supported_sasl_mechanisms=None,
        )

        kafka_no_tls_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=False,
            authentication_method="None",
            supported_sasl_mechanisms=None,
        )

        pp_interface = PandaproxyInterface(
            tls_enabled=True,
            mutual_tls_enabled=True,
            authorization_enabled=False,
            authentication_methods=[],
            configured_authentication_method="None",
        )

        report = Admin(self.redpanda).security_report()
        validate_report(
            report,
            kafka_expected={
                "dnslistener": kafka_tls_interface,
                "iplistener": kafka_tls_interface,
                "kerberoslistener": kafka_no_tls_interface,
            },
            pandaproxy_expected={"": pp_interface},
        )


class PandaproxyAuthSecurityReportTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        pass

    def _cluster_setup(self, auto_auth, enable_auth):
        security = SecurityConfig()
        security.http_authentication = ["BASIC", "OIDC"]
        security.enable_sasl = True
        security.auto_auth = auto_auth
        pandaproxy_config = PandaproxyConfig()
        pandaproxy_config.authn_method = "http_basic" if enable_auth else "none"

        self.redpanda.set_security_settings(security)
        self.redpanda.set_pandaproxy_settings(pandaproxy_config)
        super().setUp()

    @cluster(num_nodes=3)
    @matrix(auto_auth=[True, False], enable_auth=[True, False])
    def test_security_report(self, auto_auth, enable_auth):
        self._cluster_setup(auto_auth, enable_auth)

        kafka_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=True,
            authentication_method="SASL",
            supported_sasl_mechanisms=sasl_default_mechs,
        )

        if enable_auth:
            config_authn_method = "SCRAM_Proxied"
        elif not auto_auth:
            config_authn_method = "SCRAM_Configured"
        else:
            config_authn_method = "None"

        pp_interface = PandaproxyInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=enable_auth,
            authentication_methods=["BASIC", "OIDC"] if enable_auth else [],
            configured_authentication_method=config_authn_method,
        )

        expected_alerts = []
        if config_authn_method == "SCRAM_Configured":
            expected_alerts.append(
                SecurityAlert(
                    affected_interface="pandaproxy",
                    listener_name="{{unnamed}}",
                    issue="PP_CONFIGURED_CLIENT",
                    description='"pandaproxy" interface "{{unnamed}}", authorization is not enabled and the pandaproxy client has scram credentials.'
                    " This is insecure and not recommended.",
                )
            )

        report = Admin(self.redpanda).security_report()
        validate_report(
            report,
            kafka_expected={
                "dnslistener": kafka_interface,
                "iplistener": kafka_interface,
                "kerberoslistener": kafka_interface,
            },
            pandaproxy_expected={"": pp_interface},
            expected_alerts=expected_alerts,
        )


class SchemaRegistryNoSecurityReportTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, schema_registry_config=SchemaRegistryConfig(), **kwargs)

    def setUp(self):
        super().setUp()

    @cluster(num_nodes=3)
    def test_security_report(self):
        expected_alerts = [
            SecurityAlert(
                affected_interface="schema_registry",
                listener_name="{{unnamed}}",
                issue="NO_TLS",
                description='"schema_registry" interface "{{unnamed}}" is not using TLS.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="schema_registry",
                listener_name="{{unnamed}}",
                issue="NO_AUTHN",
                description='"schema_registry" interface "{{unnamed}}" is not using authentication.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="schema_registry",
                listener_name="{{unnamed}}",
                issue="NO_AUTHZ",
                description='"schema_registry" interface "{{unnamed}}" is not using authorization.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="schema_registry_client",
                listener_name="schema_registry_client",
                issue="NO_TLS",
                description='"schema_registry_client" interface "schema_registry_client" is not using TLS.'
                " This is insecure and not recommended.",
            ),
            SecurityAlert(
                affected_interface="schema_registry_client",
                listener_name="schema_registry_client",
                issue="NO_AUTHN",
                description='"schema_registry_client" interface "schema_registry_client" is not using authentication.'
                " This is insecure and not recommended.",
            ),
        ]

        report = Admin(self.redpanda).security_report()
        validate_report(
            report, schema_registry_expected={}, expected_alerts=expected_alerts
        )


class SchemaRegistryMTLSSecurityReportTest(SchemaRegistryMTLSBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.setup_cluster()

    @cluster(num_nodes=3)
    def test_security_report(self):
        kafka_tls_interface = KafkaInterface(
            tls_enabled=True,
            mutual_tls_enabled=True,
            authorization_enabled=False,
            authentication_method="mTLS",
            supported_sasl_mechanisms=None,
        )

        kafka_no_tls_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=False,
            authentication_method="None",
            supported_sasl_mechanisms=None,
        )

        sr_interface = SchemaRegistryInterface(
            tls_enabled=True,
            mutual_tls_enabled=True,
            authorization_enabled=False,
            authentication_methods=[],
        )

        src_interface = KafkaClientInterface(
            tls_enabled=True,
            mutual_tls_enabled=True,
            configured_authentication_method="None",
        )

        report = Admin(self.redpanda).security_report()
        validate_report(
            report,
            kafka_expected={
                "dnslistener": kafka_tls_interface,
                "iplistener": kafka_tls_interface,
                "kerberoslistener": kafka_no_tls_interface,
            },
            schema_registry_expected={"": sr_interface},
            schema_registry_client_expected=src_interface,
        )


class SchemaRegistryAuthSecurityReportTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            extra_rp_conf={
                "schema_registry_enable_authorization": True,
            },
            **kwargs,
        )

    def setUp(self):
        pass

    def _cluster_setup(self, auto_auth):
        security = SecurityConfig()
        security.http_authentication = ["BASIC", "OIDC"]
        security.enable_sasl = True
        security.auto_auth = auto_auth
        schema_registry_config = SchemaRegistryConfig()
        schema_registry_config.authn_method = "http_basic"

        self.redpanda.set_security_settings(security)
        self.redpanda.set_schema_registry_settings(schema_registry_config)
        super().setUp()

    @cluster(num_nodes=3)
    @matrix(auto_auth=[True, False])
    def test_security_report(self, auto_auth):
        self._cluster_setup(auto_auth)

        kafka_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=True,
            authentication_method="SASL",
            supported_sasl_mechanisms=sasl_default_mechs,
        )

        sr_interface = SchemaRegistryInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=True,
            authentication_methods=["BASIC", "OIDC"],
        )

        src_interface = KafkaClientInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            configured_authentication_method="SCRAM_Ephemeral"
            if auto_auth
            else "SCRAM_Configured",
        )

        report = Admin(self.redpanda).security_report()
        validate_report(
            report,
            kafka_expected={
                "dnslistener": kafka_interface,
                "iplistener": kafka_interface,
                "kerberoslistener": kafka_interface,
            },
            schema_registry_expected={"": sr_interface},
            schema_registry_client_expected=src_interface,
        )


class SchemaRegistryClientSecurityReportTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, schema_registry_config=SchemaRegistryConfig(), **kwargs)

    def setUp(self):
        super().setUp()

    @cluster(num_nodes=3)
    def test_security_report(self):
        report = Admin(self.redpanda).security_report()
        validate_report(report, schema_registry_expected={})


class AuditlogClientSecurityReportTest(AuditLogTestBase):
    def __init__(self, *args, **kwargs):
        security: AuditLogTestSecurityConfig = (
            AuditLogTestSecurityConfig.default_credentials()
        )
        super().__init__(
            *args,
            audit_log_config=AuditLogConfig(use_rpc=False),
            security=security,
            **kwargs,
        )

    def setUp(self, wait_for_audit_log: bool = True):
        super().setUp(wait_for_audit_log)

    @skip_fips_mode
    @cluster(num_nodes=4)
    def test_security_report(self):
        kafka_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=True,
            authentication_method="SASL",
            supported_sasl_mechanisms=sasl_default_mechs,
        )

        krb_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=True,
            # kafka_authorization_method is set but no
            # authentication method is set for this listener
            authentication_method="None",
            supported_sasl_mechanisms=None,
        )

        audit_log_expected = KafkaClientInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            configured_authentication_method="SCRAM_Ephemeral",
        )

        report = Admin(self.redpanda).security_report()
        validate_report(
            report,
            kafka_expected={
                "dnslistener": kafka_interface,
                "iplistener": kafka_interface,
                "kerberoslistener": krb_interface,
            },
            audit_log_expected=audit_log_expected,
        )
