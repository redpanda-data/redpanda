import json
import os
import socket

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService

KADM5_ACL_TMPL = """
{kadmin_principal}@{realm} *
noPermissions@{realm} X
"""
KADM5_ACL_PATH = "/etc/krb5kdc/kadm5.acl"

KDC_CONF_TMPL = """
[realms]
	{realm} = {{
		acl_file = {kadm5_acl_path}
		max_renewable_life = 7d 0h 0m 0s
		supported_enctypes = {supported_encryption_types}
		default_principal_flags = +preauth
}}
[logging]
    kdc = FILE=/var/log/kdc.log
    admin_server = FILE=/var/log/kadmin.log
"""

KDC_CONF_PATH = "/etc/krb5kdc/kdc.conf"

KRB5_CONF_TMPL = """
[libdefaults]
	default_realm = {realm}

[realms]
	{realm} = {{
		kdc_ports = 88,750
		kadmind_port = 749
		kdc = {node.account.hostname}
		admin_server = {node.account.hostname}
	}}
"""
KRB5_CONF_PATH = "/etc/krb5.conf"
DEFAULT_KEYTAB_FILE = "/etc/krb5.keytab"

KRB5KDC_PID_PATH = "/var/run/krb5kdc.pid"
KADMIND_PID_PATH = "/var/run/kadmind.pid"
KDC_DB_PATH = "/var/lib/krb5kdc/principal"

REMOTE_KADMIN_TMPL = 'kadmin -r {realm} -p {principal} -w {password} -q "{command}"'
LOCAL_KADMIN_TMPL = 'kadmin.local -q "{cmd}"'
ADD_PRINCIPAL_TMPL = 'add_principal {keytype} {principal}'
KTADD_TMPL = 'ktadd -k {keytab_file} {principal}'
KINIT_TMPL = 'kinit {principal} -kt {keytab_file}'

CLIENT_PROPERTIES_TPL = """
security.protocol=SASL_PLAINTEXT
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=redpanda
sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required \
    useKeyTab=true \
    storeKey=false \
    keyTab="{keytab_file}" \
    principal="{principal}" \
    useTicketCache=false;
"""


class AuthenticationError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)


def render_krb5_config(kdc_node, realm: str, file_path: str = KRB5_CONF_PATH):
    return KRB5_CONF_TMPL.format(node=kdc_node, realm=realm)


def render_remote_kadmin_command(command, realm, principal, password):
    return REMOTE_KADMIN_TMPL.format(realm=realm,
                                     principal=principal,
                                     password=password,
                                     command=command)


def render_local_kadmin_command(command):
    return LOCAL_KADMIN_TMPL.format(command=command)


def render_add_principal_command(principal: str, password: str = None):
    if password is None:
        keytype = '-randkey'
    else:
        keytype = f'-pw {password}'

    return ADD_PRINCIPAL_TMPL.format(keytype=keytype, principal=principal)


def render_ktadd_command(principal: str,
                         keytab_file: str = DEFAULT_KEYTAB_FILE):
    return KTADD_TMPL.format(keytab_file=keytab_file, principal=principal)


def render_kinit_command(principal: str,
                         keytab_file: str = DEFAULT_KEYTAB_FILE):
    return KINIT_TMPL.format(principal=principal, keytab_file=keytab_file)


class KrbKdc(Service):
    logs = {
        "kdc_log": {
            "path": "/var/log/kdc.log",
            "collect_default": True
        },
        "kadmin_log": {
            "path": "/var/log/kadmin.log",
            "collect_default": True
        }
    }
    """
    A Kerberos KDC implementation backed by krb5-kdc (MIT).
    """
    def __init__(self, context, realm="example.com", log_level="DEBUG"):
        super(KrbKdc, self).__init__(context, num_nodes=1)
        self.realm = realm
        self.supported_encryption_types = "aes256-cts-hmac-sha1-96:normal"
        self.kadmin_principal = "kadmin/admin"
        self.kadmin_password = "adminpassword"
        self.kadm5_acl_path = KADM5_ACL_PATH
        self.kdc_conf_path = KDC_CONF_PATH
        self.krb5_conf_path = KRB5_CONF_PATH
        self.log_level = log_level

    def _render_cfg(self, node):
        tmpl = KRB5_CONF_TMPL.format(node=node, realm=self.realm)
        self.logger.info(f"{self.krb5_conf_path}: {tmpl}")
        node.account.create_file(self.krb5_conf_path, tmpl)

        tmpl = KDC_CONF_TMPL.format(
            realm=self.realm,
            kadm5_acl_path=self.kadm5_acl_path,
            supported_encryption_types=self.supported_encryption_types)
        self.logger.info(f"{self.kdc_conf_path}: {tmpl}")
        node.account.create_file(self.kdc_conf_path, tmpl)

        tmpl = KADM5_ACL_TMPL.format(realm=self.realm,
                                     kadmin_principal=self.kadmin_principal)
        self.logger.info(f"{self.kadm5_acl_path}: {tmpl}")
        node.account.create_file(self.kadm5_acl_path, tmpl)

    def _create_or_update_principal(self, principal: str, password: str):
        if self._principal_exists(principal):
            self.logger.debug(
                f"Principal {principal} already exists.. changing password")
            self.change_principal_password(principal, password)
        else:
            self.logger.debug(
                f"Principal {principal} does not exist. Creating")
            self.add_principal(principal, password)

    def _form_kadmin_cmd(self, cmd):
        KADMIN_CMD_TMPL = 'kadmin.local -q "{cmd}"'
        return KADMIN_CMD_TMPL.format(cmd=cmd)

    def _hard_delete_principals(self, node):
        node.account.ssh(f"rm -fr {KDC_DB_PATH}*", allow_fail=True)

    def start_cmd(self):
        cmd = f"krb5kdc -P {KRB5KDC_PID_PATH} && kadmind -P {KADMIND_PID_PATH}"
        return cmd

    def _init_realm(self, node):
        self._hard_delete_principals(node)
        master_password = "1NSMkA4W7TBYapV9lC2MMeUcAEJDGK"
        cmd = f"""kdb5_util create -s<<EOF
{master_password}
{master_password}
EOF
"""
        node.account.ssh(cmd, allow_fail=False)

    def _principal_exists(self, principal: str):
        principals = self.list_principals()
        return principal in principals

    def pids(self, node):
        def pid(path: str):
            try:
                it = node.account.ssh_capture(f"cat {path}",
                                              allow_fail=False,
                                              callback=int)
                p = next(it)
                if node.account.alive(p):
                    return [p]

            except (RemoteCommandError, ValueError):
                self.logger.warn(f"pidfile not found: {path}")

            return []

        return pid(KRB5KDC_PID_PATH) + pid(KADMIND_PID_PATH)

    def alive(self, node):
        return len(self.pids(node)) == 2

    def start_node(self, node, **kwargs):
        self._render_cfg(node)
        self._init_realm(node)
        node.account.ssh(self.start_cmd(), allow_fail=False)

        wait_until(lambda: self.alive(node),
                   timeout_sec=30,
                   backoff_sec=.5,
                   err_msg="kdc took too long to start.")
        self.logger.debug("kdc is alive")
        self._create_or_update_principal(
            f"{self.kadmin_principal}@{self.realm}", self.kadmin_password)
        self._create_or_update_principal(f"noPermissions@{self.realm}",
                                         self.kadmin_password)

    def stop_node(self, node, clean_shutdown=True):
        s = "TERM" if clean_shutdown else "KILL"
        self.logger.warn(f"Stopping node {node.name}")
        for p in self.pids(node):
            node.account.ssh(f"kill -s {s} {p}", allow_fail=not clean_shutdown)

        wait_until(lambda: not self.alive(node),
                   timeout_sec=30,
                   backoff_sec=.5,
                   err_msg="kdc took too long to stop.")

    def clean_node(self, node, **kwargs):
        self.logger.warn(f"Cleaning KDC node {node.name}")
        if self.alive(node):
            self.logger.warn(
                "kdc was still alive at cleanup time. Killing forcefully...")
            self.stop_node(node, False)
        self._hard_delete_principals(node)

    def add_principal(self, principal: str, password: str):
        self.nodes[0].account.ssh(
            self._form_kadmin_cmd(f"add_principal -pw {password} {principal}"),
            allow_fail=False)

    def add_principal_randkey(self, principal: str):
        self.nodes[0].account.ssh(
            self._form_kadmin_cmd(f"add_principal -randkey {principal}"),
            allow_fail=False)

    def change_principal_password(self, principal: str, password: str):
        self.nodes[0].account.ssh(
            self._form_kadmin_cmd(f"cpw -pw {password} {principal}"),
            allow_fail=False)

    def delete_principal(self, principal: str):
        self.nodes[0].account.ssh(
            self._form_kadmin_cmd(f"delete_principal -force {principal}"),
            allow_fail=False)

    def list_principals(self):
        princs = self.nodes[0].account.ssh_capture(
            self._form_kadmin_cmd("list_principals"),
            allow_fail=False,
            callback=lambda l: l.strip())
        # Drop the first line, which is login details
        return list(princs)[1:]


class KrbClient(Service):
    """
    A Kerberos KDC implementation backed by krb5-kdc (MIT).
    """
    def __init__(self, context, kdc: KrbKdc, redpanda):
        super(KrbClient, self).__init__(context, num_nodes=1)
        self.kdc = kdc
        self.redpanda = redpanda
        self.krb5_conf_path = KRB5_CONF_PATH
        self.keytab_file = DEFAULT_KEYTAB_FILE

    def _form_kadmin_command(self,
                             command,
                             realm=None,
                             principal=None,
                             password=None):
        KADMIN_CMD_TMPL = 'kadmin -r {realm} -p {principal} -w {password} -q "{command}"'

        if realm is None:
            realm = self.kdc.realm

        if principal is None:
            principal = self.kdc.kadmin_principal

        if password is None:
            password = self.kdc.kadmin_password

        return KADMIN_CMD_TMPL.format(realm=realm,
                                      principal=principal,
                                      password=password,
                                      command=command)

    def add_principal_randkey(self, principal: str, node=None):
        cmd = self._form_kadmin_command(f"add_principal -randkey {principal}")
        if node is None:
            node = self.nodes[0]
        self.logger.debug(
            f"Adding principal {principal} remotely from node {node.name}")
        node.account.ssh(cmd=cmd, allow_fail=False)

    def delete_principal(self, principal: str, node=None):
        cmd = self._form_kadmin_command(f"delete_principal -force {principal}")
        if node is None:
            node = self.nodes[0]
        self.logger.debug(
            f"Removing principal {principal} remotely from node {node.name}")
        node.account.ssh(cmd=cmd, allow_fail=False)

    def list_principals(self, node=None):
        cmd = self._form_kadmin_command("list_principals")
        if node is None:
            node = self.nodes[0]
        princs = node.account.ssh_capture(cmd=cmd,
                                          allow_fail=False,
                                          callback=lambda l: l.strip())
        # Drop the first line, which is login details
        return list(princs)[1:]

    def start_node(self, node, **kwargs):
        self.logger.debug(f"Generating KRB5 config file for {node.name}")
        krb5_config = render_krb5_config(kdc_node=self.kdc.nodes[0],
                                         realm=self.kdc.realm)
        self.logger.debug(f"KRB5 config to {KRB5_CONF_PATH}: {krb5_config}")
        node.account.create_file(KRB5_CONF_PATH, krb5_config)

    def stop_node(self, node, clean_shutdown=True):
        self.logger.warn(f"Stopping node {node.name}")

    def clean_node(self, node, **kwargs):
        self.logger.warn(f"Cleaning Client node {node.name}")
        node.account.ssh(
            f"rm -fr {self.redpanda.PERSISTENT_ROOT}/client.keytab /etc/krb5.keytab",
            allow_fail=True)

    def add_primary(self, primary: str, realm: str = None):
        self.logger.info(
            f"Adding primary {primary} to KrbClient {self.nodes[0].name}")
        if realm is None:
            realm = self.kdc.realm
        principal = f"{primary}@{realm}"
        cmd = render_remote_kadmin_command(
            command=render_add_principal_command(principal=principal),
            realm=self.kdc.realm,
            principal=self.kdc.kadmin_principal,
            password=self.kdc.kadmin_password)
        self.logger.debug(f"Client add primary command: {cmd}")
        self.nodes[0].account.ssh(cmd=cmd, allow_fail=False)
        cmd = render_remote_kadmin_command(command=render_ktadd_command(
            principal=principal, keytab_file=self.keytab_file),
                                           realm=self.kdc.realm,
                                           principal=self.kdc.kadmin_principal,
                                           password=self.kdc.kadmin_password)
        self.logger.debug(f"Client ktadd command: {cmd}")
        self.nodes[0].account.ssh(cmd=cmd, allow_fail=False)

    def metadata(self, principal: str):
        self.logger.info("Metadata request")
        client_cache = f"/tmp/{principal}.krb5ccache"
        kinit_args = f"-kt {self.keytab_file} -c {client_cache} {principal}"
        kinit_cmd = f"kinit -R {kinit_args} || kinit {kinit_args}"
        sasl_conf = f"-X security.protocol=sasl_plaintext -X sasl.mechanisms=GSSAPI '-Xsasl.kerberos.kinit.cmd={kinit_cmd}' -X sasl.kerberos.service.name=redpanda"
        try:
            res = self.nodes[0].account.ssh_output(
                cmd=
                f'KRB5_TRACE=/dev/stderr KRB5CCNAME={client_cache} kcat -L -J -b {self.redpanda.brokers(listener="kerberoslistener")} {sasl_conf}',
                allow_fail=False,
                combine_stderr=False)
            self.logger.debug(f"Metadata request: {res}")
            return json.loads(res)
        except RemoteCommandError as err:
            if b'No Kerberos credentials available' in err.msg:
                raise AuthenticationError(err.msg) from err
            raise

    def metadata_java(self, principal: str):
        self.logger.info("Metadata request (Java)")
        tmpl = CLIENT_PROPERTIES_TPL.format(keytab_file=self.keytab_file,
                                            principal=principal)
        properties_filepath = f"/tmp/{principal}_client.properties"
        self.nodes[0].account.create_file(properties_filepath, tmpl)

        try:
            cmd_args = f'--command-config {properties_filepath} --bootstrap-server {self.redpanda.brokers(listener="kerberoslistener")}'
            topics = self.nodes[0].account.ssh_output(
                cmd=f"/opt/kafka-3.0.0/bin/kafka-topics.sh {cmd_args} --list",
                allow_fail=False,
                combine_stderr=False)
            self.logger.debug(f"kafka-topics: {topics}")

            brokers = self.nodes[0].account.ssh_output(
                cmd=
                f"/opt/kafka-3.0.0/bin/kafka-broker-api-versions.sh {cmd_args} | awk '/^[a-z]/ {{print $1}}'",
                allow_fail=False,
                combine_stderr=False)
            self.logger.debug(f"kafka-broker-api-versions: {brokers}")

            def sanitize(raw: bytes):
                return filter(None, raw.decode('utf-8').split('\n'))

            metadata = {
                "brokers": [{
                    'name': l
                } for l in sanitize(brokers)],
                "topics": [{
                    'topic': l
                } for l in sanitize(topics)],
            }

            self.logger.debug(f"Metadata request: {metadata}")
            return metadata
        except RemoteCommandError as err:
            if b'javax.security.auth.login.LoginException' in err.msg:
                raise AuthenticationError(err.msg) from err
            raise


class RedpandaKerberosNode(RedpandaService):
    def __init__(
            self,
            context,
            kdc: KrbKdc,
            realm: str,
            keytab_file=f"{RedpandaService.PERSISTENT_ROOT}/redpanda.keytab",
            *args,
            **kwargs):
        super(RedpandaKerberosNode, self).__init__(context, *args, **kwargs)
        self.kdc = kdc
        self.realm = realm
        self.keytab_file = keytab_file

    def clean_node(self, node, **kwargs):
        super().clean_node(node, **kwargs)
        node.account.ssh(f"rm -fr {self.keytab_file}", allow_fail=True)

    def start_node(self, node, **kwargs):
        self.logger.debug(
            f"Rendering KRB5 config for {node.name} using KDC node {self.kdc.nodes[0].name}"
        )
        krb5_config = render_krb5_config(kdc_node=self.kdc.nodes[0],
                                         realm=self.kdc.realm)
        self.logger.debug(f"KRB5 config to {KRB5_CONF_PATH}: {krb5_config}")
        node.account.create_file(KRB5_CONF_PATH, krb5_config)
        principal = self._service_principal(node)
        self.logger.debug(f"Principal for {node.name}: {principal}")
        self.logger.debug(f"Adding principal {principal} to KDC")
        cmd = render_remote_kadmin_command(
            command=render_add_principal_command(principal=principal),
            realm=self.kdc.realm,
            principal=self.kdc.kadmin_principal,
            password=self.kdc.kadmin_password)
        self.logger.debug(f"principal add command: {cmd}")
        node.account.ssh(cmd=cmd, allow_fail=False)
        self.logger.debug(
            f"Generating keytab file {self.keytab_file} for {node.name}")
        cmd = render_remote_kadmin_command(command=render_ktadd_command(
            principal=principal, keytab_file=self.keytab_file),
                                           realm=self.kdc.realm,
                                           principal=self.kdc.kadmin_principal,
                                           password=self.kdc.kadmin_password)
        self.logger.debug(f"ktadd command: {cmd}")
        node.account.ssh(f"mkdir -p {os.path.dirname(self.keytab_file)}")
        node.account.ssh(cmd=cmd, allow_fail=False)
        super().start_node(node, **kwargs)

    def _service_principal(self, node, primary: str = "redpanda"):
        fqdn = RedpandaService.get_node_fqdn(node)
        return f"{primary}/{fqdn}@{self.realm}"
