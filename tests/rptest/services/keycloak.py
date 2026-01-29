import json
import os
import tempfile

import requests
from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from keycloak import KeycloakAdmin
from keycloak.exceptions import KeycloakGetError

KC_INSTALL_DIR = os.path.join("/", "opt", "keycloak")
KC_DATA_DIR = os.path.join(KC_INSTALL_DIR, "data")
KC_VAULT_DIR = os.path.join(KC_DATA_DIR, "vault")
KC_BIN_DIR = os.path.join(KC_INSTALL_DIR, "bin")
KC_CONF_DIR = os.path.join(KC_INSTALL_DIR, "conf")
KC = os.path.join(KC_BIN_DIR, "kc.sh")
KCADM = os.path.join(KC_BIN_DIR, "kcadm.sh")
KC_CFG = "keycloak.conf"
KC_TLS_KEY_FILE = os.path.join(KC_CONF_DIR, "key.pem")
KC_TLS_CRT_FILE = os.path.join(KC_CONF_DIR, "crt.pem")
KC_TLS_CA_CRT_FILE = os.path.join(KC_CONF_DIR, "ca.crt")
KC_TLS_TRUST_STORE = os.path.join(KC_CONF_DIR, "kcTrustStore.jks")
KC_ADMIN = "admin"
KC_ADMIN_PASSWORD = "admin"
KC_ROOT_LOG_LEVEL = "INFO"
KC_LOG_HANDLER = "console,file"
KC_LOG_FILE = "/var/log/kc.log"
KC_PORT = 8080

# Sort of arbitrary. Just make sure it's in [8000, 8100] or update
# ingress rules in vtools/qa/deploy/terraform
KC_HTTPS_PORT = 8093

DEFAULT_REALM = "demorealm"

DEFAULT_AT_LIFESPAN_S = 600

START_CMD_TMPL = """
LAUNCH_JBOSS_IN_BACKGROUND=1 \
KC_BOOTSTRAP_ADMIN_USERNAME={admin} \
KC_BOOTSTRAP_ADMIN_PASSWORD={pw} \
{kc} start-dev &
"""

OIDC_CONFIG_TMPL = """\
{scheme}://{host}:{port}/realms/{realm}/.well-known/openid-configuration\
"""

DEFAULT_CONFIG = {
    "http-port": KC_PORT,
    "hostname": None,
    "hostname-port": KC_PORT,
    "http-enabled": True,
    "proxy": "passthrough",
    "log": KC_LOG_HANDLER,
    "log-file": KC_LOG_FILE,
    "log-level": KC_ROOT_LOG_LEVEL,
}


class KeycloakConfigWriter:
    _cfg = DEFAULT_CONFIG.copy()

    def __init__(self, dest_dir: str = KC_CONF_DIR, extra_cfg: dict = {}):
        self._cfg.update(extra_cfg)
        self._dest_dir = dest_dir

    @property
    def rep(self):
        return json.dumps(self._cfg)

    def write(self, node):
        try:
            node.account.mkdirs(self._dest_dir)
        except Exception:
            pass
        dest_f = os.path.join(self._dest_dir, KC_CFG)
        with tempfile.NamedTemporaryFile(mode="w") as f:
            for k, v in self._cfg.items():
                if v is None:
                    continue
                f.write(f"{k}={str(v).lower() if type(v) is bool else v}\n")
            f.flush()
            node.account.copy_to(f.name, dest_f)
        return dest_f


class OAuthConfig:
    def __init__(
        self, client_id, client_secret, token_endpoint, ca_cert=None, scopes=["openid"]
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_endpoint = token_endpoint
        self.ca_cert = ca_cert
        self.scopes = scopes


class KeycloakAdminClient:
    GROUP_MAPPER_NAME = "groups-mapper"

    def __init__(
        self,
        logger,
        server_url,
        realm,
        username=KC_ADMIN,
        password=KC_ADMIN_PASSWORD,
        access_token_lifespan_s=DEFAULT_AT_LIFESPAN_S,
    ):
        self.logger = logger
        self.logger.debug(f"KeycloakAdminClient for {server_url}")
        self.kc_admin = KeycloakAdmin(
            server_url=server_url,
            username=username,
            password=password,
            realm_name="master",
        )
        self.kc_admin.create_realm(
            payload={
                "realm": realm,
                "enabled": True,
                "accessTokenLifespan": access_token_lifespan_s,
                "requiredActions": [],
            }
        )
        self.kc_admin.change_current_realm(realm_name=realm)

    def config(self, server_url, username, password, realm):
        self.kc_admin = KeycloakAdmin(
            server_url=server_url,
            username=username,
            password=password,
            realm_name=realm,
        )

    def create_client(self, client_id, **kwargs):
        rep = {
            "clientId": client_id,
            "enabled": True,
            "serviceAccountsEnabled": True,  # for client credentials grant
        }
        rep.update(kwargs)
        id = self.kc_admin.create_client(payload=rep)
        self.logger.debug(f"client_id: {id}")
        return id

    def create_group_mapper(self, client_id: str, use_full_path: bool = True):
        id = self.kc_admin.get_client_id(client_id)
        assert id is not None, f"Client {client_id} not found"
        self.logger.debug(f"Creating group mapper for client {client_id} (id: {id})")

        try:
            mappers = self.kc_admin.get_mappers_from_client(id)
            mapper = next(
                (m for m in mappers if m["name"] == self.GROUP_MAPPER_NAME), None
            )
        except KeycloakGetError:
            mapper = None

        mapper_representation = {
            "name": self.GROUP_MAPPER_NAME,
            "protocol": "openid-connect",
            "protocolMapper": "oidc-group-membership-mapper",
            "config": {
                "access.token.claim": "true",
                "id.token.claim": "true",
                "userinfo.token.claim": "true",
                "full.path": f"{use_full_path}".lower(),
                "claim.name": "groups",
                "jsonType.label": "String",
            },
        }

        if mapper is None:
            self.logger.debug("Creating group mapper")
            self.kc_admin.add_mapper_to_client(id, mapper_representation)

    def create_group(
        self,
        group_name: str,
        parent: str | None = None,
        skip_exists: bool = True,
        **kwargs,
    ) -> str:
        self.logger.debug(f"Creating group named {group_name}")
        payload = {"name": group_name}
        payload.update(kwargs)

        group_id = self.kc_admin.create_group(
            payload=payload, parent=parent, skip_exists=skip_exists
        )

        if group_id is None:
            if not skip_exists:
                raise RuntimeError(f"Failed to create group named {group_name}")
            group_id = self.kc_admin.get_groups({"name": group_name})[0]["id"]

        return group_id

    def _get_group_id(self, group_name: str) -> str | None:
        groups = self.kc_admin.get_groups()
        group = next(g for g in groups if g["name"] == group_name)
        if group is None:
            return None
        return group["id"]

    def _add_user_to_group(self, user_id: str, group_id: str):
        self.kc_admin.group_user_add(user_id=user_id, group_id=group_id)

    def _remove_user_from_group(self, user_id: str, group_id: str):
        self.kc_admin.group_user_remove(user_id=user_id, group_id=group_id)

    def add_service_user_to_group(self, client_id: str, group_name: str):
        id = self.kc_admin.get_client_id(client_id)
        assert id is not None, f"Client {client_id} not found"
        service_account_user = self.kc_admin.get_client_service_account_user(id)
        account_id = service_account_user["id"]
        group_id = self._get_group_id(group_name=group_name)
        assert group_id is not None, f"Group {group_name} not found"
        self._add_user_to_group(user_id=account_id, group_id=group_id)

    def remove_service_user_from_group(self, client_id: str, group_name: str):
        id = self.kc_admin.get_client_id(client_id)
        assert id is not None, f"Client {client_id} not found"
        service_account_user = self.kc_admin.get_client_service_account_user(id)
        account_id = service_account_user["id"]
        group_id = self._get_group_id(group_name=group_name)
        assert group_id is not None, f"Group {group_name} not found"
        self._remove_user_from_group(user_id=account_id, group_id=group_id)

    def generate_client_secret(self, client_id):
        id = self.kc_admin.get_client_id(client_id)
        assert id is not None, f"Client {client_id} not found"
        self.kc_admin.generate_client_secrets(id)

    def get_client_secret(self, client_id):
        id = self.kc_admin.get_client_id(client_id)
        assert id is not None, f"Client {client_id} not found"
        secret = self.kc_admin.get_client_secrets(id)
        return secret["value"]

    def create_user(self, username, password, realm_admin=False, **kwargs):
        rep = {
            "username": username,
            "credentials": [
                {
                    "type": "password",
                    "value": password,
                    "temporary": False,
                }
            ],
            "enabled": True,
        }
        rep.update(kwargs)

        user_id = self.kc_admin.create_user(rep)

        if realm_admin:
            client_id = self.kc_admin.get_client_id("realm-management")
            assert client_id is not None, "Client realm-management not found"
            role_id = self.kc_admin.get_client_role_id(
                client_id=client_id, role_name="realm-admin"
            )
            assert role_id is not None, "Role realm-admin not found"
            self.kc_admin.assign_client_role(
                user_id=user_id,
                client_id=client_id,
                roles=[{"name": "realm-admin", "id": role_id}],
            )
        return user_id

    def update_user(self, username, **kwargs):
        user_id = self.kc_admin.get_user_id(username)
        if user_id is None:
            raise Exception(f"User {username} not found")

        self.kc_admin.update_user(user_id=user_id, payload=kwargs)


class KeycloakService(Service):
    logs = {
        "keycloak_log": {
            "path": f"{KC_LOG_FILE}",
            "collect_default": True,
        },
    }

    def __init__(
        self,
        context,
        port: int = KC_PORT,
        https_port: int = KC_HTTPS_PORT,
        realm: str = DEFAULT_REALM,
        log_level: str = KC_ROOT_LOG_LEVEL,
        tls=None,
    ):
        super(KeycloakService, self).__init__(context, num_nodes=1)
        self.realm = realm
        self.http_port = port
        self.log_level = log_level
        self._admin = None
        self._remote_config_file = None

        self.tls_provider = tls
        self.https_port = None
        if self.tls_provider is not None:
            self.https_port = https_port

    @property
    def admin(self) -> KeycloakAdminClient:
        assert self._admin is not None
        return self._admin

    @property
    def admin_ll(self):
        return self.admin.kc_admin

    def _start_cmd(self):
        cmd = START_CMD_TMPL.format(admin=KC_ADMIN, pw=KC_ADMIN_PASSWORD, kc=KC)
        self.logger.debug(f"KC START CMD: {cmd}")
        return cmd

    def host(self, node):
        return node.account.hostname

    def get_discovery_url(self, node, use_ssl=False):
        return OIDC_CONFIG_TMPL.format(
            scheme="https" if use_ssl else "http",
            host=self.host(node),
            port=self.https_port if use_ssl else self.http_port,
            realm=self.realm,
        )

    def get_token_endpoint(self, node, use_ssl=False):
        ca_cert = self.tls_provider.ca.crt if self.tls_provider is not None else True
        oidc_config = requests.get(
            self.get_discovery_url(node=node, use_ssl=use_ssl), verify=ca_cert
        ).json()
        return oidc_config["token_endpoint"]

    def login_admin_user(self, node, username, password):
        self.admin.config(
            server_url=f"http://{self.host(node)}:{self.http_port}",
            username=username,
            password=password,
            realm=self.realm,
        )

    def generate_oauth_config(self, node, client_id):
        secret = self.admin.get_client_secret(client_id)
        use_ssl = self.tls_provider is not None
        ca_cert = self.tls_provider.ca.crt if self.tls_provider is not None else None
        token_endpoint = self.get_token_endpoint(node, use_ssl=use_ssl)
        return OAuthConfig(client_id, secret, token_endpoint, ca_cert=ca_cert)

    def pids(self, node):
        return node.account.java_pids("quarkus")

    def alive(self, node):
        return len(self.pids(node)) > 0

    @property
    def using_tls(self):
        return self.tls_provider is not None

    @property
    def hostname_port(self):
        assert not self.using_tls or self.https_port is not None
        return self.https_port if self.using_tls else self.http_port

    def setup_tls(self, node) -> dict:
        if not self.using_tls:
            return {}
        assert self.https_port is not None and self.tls_provider is not None
        cert = self.tls_provider.create_broker_cert(self, node)
        node.account.copy_to(cert.crt, KC_TLS_CRT_FILE)
        node.account.copy_to(cert.key, KC_TLS_KEY_FILE)
        node.account.copy_to(cert.ca.crt, KC_TLS_CA_CRT_FILE)
        for f in [KC_TLS_CRT_FILE, KC_TLS_KEY_FILE, KC_TLS_CA_CRT_FILE]:
            node.account.ssh(f"chmod 755 {f}", allow_fail=False)

        storepass = "passwd"
        node.account.ssh(
            f"keytool -import -file {KC_TLS_CA_CRT_FILE} -alias rpCA -keystore {KC_TLS_TRUST_STORE} -storepass {storepass} -noprompt",
            allow_fail=False,
        )
        # extra parameters for the config file writer
        return {
            "https-certificate-file": KC_TLS_CRT_FILE,
            "https-certificate-key-file": KC_TLS_KEY_FILE,
            "https-trust-store-file": KC_TLS_TRUST_STORE,
            "https-trust-store-password": storepass,
            "https-port": self.https_port,
        }

    def start_node(self, node, access_token_lifespan_s=DEFAULT_AT_LIFESPAN_S, **kwargs):
        scheme = "https" if self.using_tls else "http"
        extra_cfg = {
            "hostname": f"{scheme}://{self.host(node)}:{self.hostname_port}",
            "hostname-port": self.hostname_port,
            "http-port": self.http_port,
            "log-level": self.log_level,
        }
        extra_cfg.update(self.setup_tls(node))
        cfg_writer = KeycloakConfigWriter(extra_cfg=extra_cfg)
        self._remote_config_file = cfg_writer.write(node)

        node.account.ssh(f"touch {KC_LOG_FILE}", allow_fail=False)

        self.logger.debug(f"Starting Keycloak service {cfg_writer.rep}")

        with node.account.monitor_log(KC_LOG_FILE) as monitor:
            node.account.ssh(f"{KC} build", allow_fail=False)
            node.account.ssh_capture(self._start_cmd(), allow_fail=False)
            monitor.wait_until("(main) Profile dev activated.", timeout_sec=30)

        self.logger.debug(f"Keycloak PIDs: {self.pids(node)}")

        self._admin = KeycloakAdminClient(
            self.logger,
            server_url=f"http://{self.host(node)}:{self.http_port}",
            realm=self.realm,
            username=f"{KC_ADMIN}",
            password=f"{KC_ADMIN_PASSWORD}",
            access_token_lifespan_s=access_token_lifespan_s,
        )

    def stop_node(self, node, clean_shutdown=True, **_):
        s = "TERM" if clean_shutdown else "KILL"
        self.logger.warn(f"Stopping node {node.name}")

        for p in self.pids(node):
            node.account.ssh(f"kill -s {s} {p}", allow_fail=not clean_shutdown)

        wait_until(
            lambda: not self.alive(node),
            timeout_sec=30,
            backoff_sec=0.5,
            err_msg="Keycloak took too long to stop.",
        )

    def clean_node(self, node, **kwargs):
        self.logger.warn(f"Cleaning Keycloak node {node.name}")
        if self.alive(node):
            self.stop_node(node)

        node.account.ssh(f"rm -rf {KC_LOG_FILE}")
        node.account.ssh("rm -rf /opt/keycloak/data/*", allow_fail=False)
        if self._remote_config_file is not None:
            node.account.ssh(f"rm {self._remote_config_file}")

        node.account.ssh(
            f"rm -f {KC_TLS_CRT_FILE} {KC_TLS_KEY_FILE}  {KC_TLS_CA_CRT_FILE} {KC_TLS_TRUST_STORE}"
        )
