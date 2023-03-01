import json
import os

from collections import namedtuple
from ducktape.cluster.remoteaccount import RemoteCommandError, RemoteAccount
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
	dns_canonicalize_hostname = false

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

REMOTE_KADMIN_TMPL = 'KRB5_CONFIG={krb5_conf_path} kadmin -r {realm} -p {principal} -w {password} -q "{command}"'
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


def render_krb5_config(kdc_node, realm: str):
    return KRB5_CONF_TMPL.format(node=kdc_node, realm=realm)


def render_remote_kadmin_command(command,
                                 realm,
                                 principal,
                                 password,
                                 krb5_conf_path=KRB5_CONF_PATH):
    return REMOTE_KADMIN_TMPL.format(realm=realm,
                                     principal=principal,
                                     password=password,
                                     krb5_conf_path=krb5_conf_path,
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

    def _configure_principal(self, krb5_conf_path: str, principal: str,
                             dest: str, dest_node):
        if not "@" in principal:
            principal = f"{principal}@{self.realm}"
        self.logger.info(f"Adding principal {principal} to KDC")
        cmd = render_remote_kadmin_command(
            command=render_add_principal_command(principal=principal),
            realm=self.realm,
            principal=self.kadmin_principal,
            password=self.kadmin_password,
            krb5_conf_path=krb5_conf_path)
        self.logger.debug(f"principal add command: {cmd}")
        dest_node.account.ssh(cmd=cmd, allow_fail=False)
        self.logger.debug(
            f"Generating keytab file {dest} for {dest_node.name}")
        cmd = render_remote_kadmin_command(command=render_ktadd_command(
            principal=principal, keytab_file=dest),
                                           realm=self.realm,
                                           principal=self.kadmin_principal,
                                           password=self.kadmin_password,
                                           krb5_conf_path=krb5_conf_path)
        self.logger.debug(f"ktadd command: {cmd}")
        dest_node.account.ssh(f"mkdir -p {os.path.dirname(dest)}")
        dest_node.account.ssh(cmd=cmd, allow_fail=False)

    def configure_service_principal(self, krb5_conf_path: str, principal: str,
                                    dest: str, dest_node):
        self._configure_principal(krb5_conf_path, principal, dest, dest_node)

    def configure_user_principal(self, krb5_conf_path: str, principal: str,
                                 dest: str, dest_node):
        self._configure_principal(krb5_conf_path, principal, dest, dest_node)


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
        node.account.ssh(f"rm -fr /tmp/*.krb5ccache", allow_fail=True)

    def add_primary(self, primary: str):
        self.logger.info(
            f"Adding primary {primary} to KrbClient {self.nodes[0].name}")
        self.kdc.configure_user_principal(krb5_conf_path=self.krb5_conf_path,
                                          principal=primary,
                                          dest=self.keytab_file,
                                          dest_node=self.nodes[0])

    def metadata(self, principal: str):
        self.logger.info("Metadata request")
        client_cache = f"/tmp/{principal.replace('/', '_')[0]}.krb5ccache"
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
            krb5_conf_path=KRB5_CONF_PATH,
            *args,
            **kwargs):
        super(RedpandaKerberosNode, self).__init__(context, *args, **kwargs)
        self.kdc = kdc
        self.realm = realm
        self.keytab_file = keytab_file
        self.krb5_conf_path = krb5_conf_path

    def clean_node(self, node, **kwargs):
        super().clean_node(node, **kwargs)
        node.account.ssh(f"rm -fr {self.keytab_file}", allow_fail=True)
        if self.krb5_conf_path != KRB5_CONF_PATH:
            node.account.ssh(f"rm -fr {self.krb5_conf_path}", allow_fail=True)

    def start_node(self, node, **kwargs):
        self.logger.debug(
            f"Rendering KRB5 config for {node.name} using KDC node {self.kdc.nodes[0].name}"
        )
        krb5_config = render_krb5_config(kdc_node=self.kdc.nodes[0],
                                         realm=self.kdc.realm)
        self.logger.debug(
            f"KRB5 config to {self.krb5_conf_path}: {krb5_config}")
        node.account.ssh(f"mkdir -p {os.path.dirname(self.krb5_conf_path)}")
        node.account.create_file(self.krb5_conf_path, krb5_config)
        principal = self._service_principal(node)
        self.logger.debug(f"Principal for {node.name}: {principal}")
        self.kdc.configure_service_principal(self.krb5_conf_path, principal,
                                             self.keytab_file, node)
        super().start_node(node, **kwargs)

    def _service_principal(self, node, primary: str = "redpanda"):
        fqdn = RedpandaService.get_node_fqdn(node)
        return f"{primary}/{fqdn}@{self.realm}"


class ActiveDirectoryKdc:
    def __init__(self, logger, remote_account: RemoteAccount, realm: str,
                 keytab_password: str, upn_user: str, spn_user: str):
        self.logger = logger
        self.remote_account = remote_account
        self.realm = realm
        self.keytab_password = keytab_password
        self.upn_user = upn_user
        self.spn_user = spn_user
        self.nodes = [
            namedtuple("node", "name account")(remote_account.hostname,
                                               remote_account)
        ]

    def ssh_input_output(self,
                         cmd,
                         allow_fail=False,
                         combine_stderr=True,
                         timeout_sec=None,
                         in_lines=None):
        """Runs the command via SSH and captures the output, returning it as a string.

        :param cmd: The remote ssh command.
        :param allow_fail: If True, ignore nonzero exit status of the remote command,
               else raise an ``RemoteCommandError``
        :param combine_stderr: If True, return output from both stderr and stdout of the remote process.
        :param timeout_sec: Set timeout on blocking reads/writes. Default None. For more details see
            http://docs.paramiko.org/en/2.0/api/channel.html#paramiko.channel.Channel.settimeout
        :param in_lines: an iterable sequence of strings to pass to stdin

        :return: The stdout output from the ssh command.
        :raise RemoteCommandError: If ``allow_fail`` is False and the command returns a non-zero exit status
        """
        self.logger.debug(f"Running ssh command: {cmd}")

        stdin, stdout, stderr = self.remote_account.ssh_client.exec_command(
            command=cmd, timeout=timeout_sec, get_pty=True)

        try:
            stdin.writelines(in_lines)
            stdin.flush()

            stdout.channel.set_combine_stderr(combine_stderr)
            stdoutdata = stdout.readlines()

            exit_status = stdin.channel.recv_exit_status()
            if exit_status != 0:
                if not allow_fail:
                    raise RemoteCommandError(self, cmd, exit_status,
                                             stderr.readlines())
                self.logger.debug(
                    f"Running ssh command '{cmd}' exited with status {exit_status} and message: {stderr.readlines()}"
                )
        finally:
            stdin.close()
            stdout.close()
            stderr.close()

        return stdoutdata

    def _configure_principal(self, krb5_conf_path: str, user: str,
                             principal: str, password: str, dest: str,
                             dest_node):
        if not "@" in principal:
            principal = f"{principal}@{self.realm}"
        src = fr"{user}.keytab.temp"
        cmd = f"ktpass -out {src} -mapuser {user} -princ {principal} -crypto ALL -pass * -ptype KRB5_NT_PRINCIPAL +DumpSalt"
        self.logger.debug(f"Configuring principal cmd: {cmd}")
        out = self.ssh_input_output(
            cmd=cmd,
            allow_fail=False,
            combine_stderr=False,
            timeout_sec=5,
            in_lines=[f"{password}\r\n", f"{password}\r\n"])
        self.logger.info(f"Output of ktpass: {out}")
        self.logger.debug(
            f"copying keytab: from {src} to {dest_node.name} at {dest}")
        dest_node.account.ssh(f"mkdir -p {os.path.dirname(dest)}")
        self.remote_account.copy_between(src, dest, dest_node)
        self.remote_account.ssh(cmd=f"del {src}", allow_fail=False)

    def configure_service_principal(self, krb5_conf_path: str, principal: str,
                                    dest: str, dest_node):
        self._configure_principal(krb5_conf_path, self.spn_user, principal,
                                  self.keytab_password, dest, dest_node)

    def configure_user_principal(self, krb5_conf_path: str, principal: str,
                                 dest: str, dest_node):
        self._configure_principal(krb5_conf_path, self.upn_user, principal,
                                  self.keytab_password, dest, dest_node)

    def start(self):
        pass
