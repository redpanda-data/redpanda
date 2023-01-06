import json
import os

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.utils.util import wait_until

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

KRB5KDC_PID_PATH = "/var/run/krb5kdc.pid"
KADMIND_PID_PATH = "/var/run/kadmind.pid"
KDC_DB_PATH = "/var/lib/krb5kdc/principal"


class KrbKdc(Service):
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

    def _hard_delete_principals(self, node):
        node.account.ssh(f"rm -fr {KDC_DB_PATH}*", allow_fail=True)

    def _init_realm(self, node):
        self._hard_delete_principals(node)
        master_password = "1NSMkA4W7TBYapV9lC2MMeUcAEJDGK"
        cmd = f"""krb5_newrealm<<EOF
{master_password}
{master_password}
EOF
"""
        node.account.ssh(cmd, allow_fail=False)

    def start_cmd(self):
        cmd = f"krb5kdc -P {KRB5KDC_PID_PATH};kadmind -P {KADMIND_PID_PATH}"
        return cmd

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
                self.logger.warn("pidfile not found: {path}")

            return []

        return pid(KRB5KDC_PID_PATH) + pid(KADMIND_PID_PATH)

    def alive(self, node):
        return len(self.pids(node)) == 2

    def start_node(self, node):
        self._render_cfg(node)
        self._init_realm(node)
        # Run kdc
        cmd = self.start_cmd()
        self.logger.debug("kdc command: %s", cmd)
        node.account.ssh(cmd, allow_fail=False)
        wait_until(lambda: self.alive(node),
                   timeout_sec=30,
                   backoff_sec=.5,
                   err_msg="kdc took too long to start.")
        self.logger.debug("kdc is alive")
        self.add_principal(f"{self.kadmin_principal}@{self.realm}",
                           self.kadmin_password)
        self.add_principal(f"noPermissions@{self.realm}", self.kadmin_password)

    def stop_node(self, node, clean_shutdown=True):
        s = "TERM" if clean_shutdown else "KILL"
        self.logger.warn(f"Stopping node {node.name}")
        for p in self.pids(node):
            node.account.ssh(f"kill -s {s} {p}", allow_fail=not clean_shutdown)

        wait_until(lambda: not self.alive(node),
                   timeout_sec=30,
                   backoff_sec=.5,
                   err_msg="kdc took too long to stop.")

    def clean_node(self, node):
        self.logger.warn(f"Cleaning node {node.name}")
        if self.alive(node):
            self.logger.warn(
                "kdc was still alive at cleanup time. Killing forcefully...")
            self.stop_node(node, False)
        self._hard_delete_principals(node)

    def add_principal(self, principal: str, password: str):
        self.nodes[0].account.ssh(
            f'kadmin.local -q "add_principal -pw {password} {principal}"',
            allow_fail=False)

    def add_principal_randkey(self, principal: str):
        self.nodes[0].account.ssh(
            f'kadmin.local -q "add_principal -randkey {principal}"',
            allow_fail=False)

    def delete_principal(self, principal: str):
        self.nodes[0].account.ssh(
            f'kadmin.local -q "delete_principal -force {principal}"',
            allow_fail=False)

    def ktadd(self, principal: str, dst: str, nodes):
        src = "/temporary.keytab"
        self.nodes[0].account.ssh(f"rm {src}", allow_fail=True)
        for node in nodes:
            # hostname = next(node.account.ssh_capture("hostname -f")).strip()
            self.nodes[0].account.ssh(
                f'kadmin.local -q "ktadd -k {src} {principal}"')
            self.logger.info(
                f"Copying: {self.nodes[0].name}:{src} -> {node.name}:{dst}")
            node.account.ssh(f"mkdir -p {os.path.dirname(dst)}")
            self.nodes[0].account.copy_between(src, dst, node)
            self.nodes[0].account.copy_between(src, "/node.keytab", node)
            node.account.ssh(f"kinit {principal} -t {dst}")
            kl = node.account.ssh_capture(f"klist -k {dst}")
            self.logger.info(f"klist: {list(kl)}")

    def list_principals(self):
        princs = self.nodes[0].account.ssh_capture(
            'kadmin.local -q "list_principals"',
            allow_fail=False,
            callback=lambda l: l.strip())
        # Drop the first line, which is login details
        return list(princs)[1:]


class KrbClient(Service):
    """
    A Kerberos KDC implementation backed by krb5-kdc (MIT).
    """
    def __init__(self, context, kdc, redpanda, principal: str):
        super(KrbClient, self).__init__(context, num_nodes=1)
        self.kdc = kdc
        self.redpanda = redpanda
        self.principal = principal
        self.krb5_conf_path = KRB5_CONF_PATH

    def _render_cfg(self, node):
        tmpl = KRB5_CONF_TMPL.format(node=self.kdc.nodes[0],
                                     realm=self.kdc.realm)
        self.logger.info(f"{self.krb5_conf_path}: {tmpl}")
        node.account.create_file(self.krb5_conf_path, tmpl)

    def start_node(self, node):
        self._render_cfg(node)

    def stop_node(self, node, clean_shutdown=True):
        self.logger.warn(f"Stopping node {node.name}")

    def clean_node(self, node):
        self.logger.warn(f"Cleaning node {node.name}")

    def metadata(self):
        self.logger.info("Metadata request")
        res = self.nodes[0].account.ssh_output(
            cmd=
            f"KRB5_TRACE=/dev/stderr kcat -L -J -b {self.redpanda.brokers()} -X security.protocol=sasl_plaintext -X sasl.mechanisms=GSSAPI '-Xsasl.kerberos.kinit.cmd=kinit {self.principal} -t {self.redpanda.PERSISTENT_ROOT}/client.keytab' -X sasl.kerberos.service.name=redpanda",
            allow_fail=False,
            combine_stderr=False)
        self.logger.debug(f"Metadata request: {res}")
        return json.loads(res)
