# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass, fields

from ducktape.mark import parametrize

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.scram_test import SaslPlainTLSProvider
from rptest.services.redpanda import SecurityConfig, RedpandaService
from rptest.services.tls import TLSCertManager


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


@dataclass
class SecurityAlert:
    affected_interface: str | None
    listener_name: str | None
    issue: str
    description: str


def validate_report(
    response, kafka_expected={}, rpc_expected=RpcInterface.default(), expected_alerts=[]
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

    alerts_json = get_key("alerts", report_json)
    alerts = [make_from_dict(SecurityAlert, a) for a in alerts_json]
    alerts_str = "\n".join(str(a) for a in alerts)

    for expected_alert in expected_alerts:
        assert expected_alert in alerts, (
            f"Expected alert:\n{expected_alert}\nnot found in alerts:\n{alerts_str}"
        )


class NoSecurityReportTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
        ]

        validate_report(report, expected_alerts=expected_alerts)


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
        sasl_mechs = sasl_plain_mechs if authz_enabled else None

        maybe_tls_interface = KafkaInterface(
            tls_enabled=enable_tls,
            mutual_tls_enabled=enable_tls,
            authorization_enabled=authz_enabled,
            authentication_method=authn,
            supported_sasl_mechanisms=sasl_mechs,
        )
        no_tls_interface = KafkaInterface(
            tls_enabled=False,
            mutual_tls_enabled=False,
            authorization_enabled=authz_enabled,
            authentication_method=authn if authz_enabled else "None",
            supported_sasl_mechanisms=sasl_mechs,
        )

        expected_alerts = []
        if authz_enabled:
            expected_alerts.extend(
                [
                    SecurityAlert(
                        affected_interface="kafka",
                        listener_name="dnslistener",
                        issue="SASL_PLAIN",
                        description='"kafka" interface "dnslistener" is using SASL/PLAIN.'
                        " This is insecure and not recommended.",
                    ),
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
                "dnslistener": maybe_tls_interface,
                "iplistener": maybe_tls_interface,
                "kerberoslistener": no_tls_interface,
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
