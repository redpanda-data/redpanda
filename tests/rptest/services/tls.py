import tempfile
import typing
import collections
import pathlib
import subprocess
import os
import string
import random

_ca_config_tmpl = """
# OpenSSL CA configuration file
[ ca ]
default_ca = local_ca

[ local_ca ]
dir              = {dir}
certificate      = $dir/ca.crt
private_key      = $dir/ca.key # CA private key
database         = $dir/index.txt
serial           = $dir/serial.txt
crlnumber        = $dir/crlnumber.txt
default_days     = 730
default_md       = sha256
copy_extensions  = copy
unique_subject   = no
default_crl_days = 365                   # How long before next CRL

# Used to create the CA certificate.
[ req ]
prompt             = no
distinguished_name = distinguished_name
x509_extensions    = extensions

[ root_ca_distinguished_name ]
commonName              = Test TLS CA
stateOrProvinceName     = NY
countryName             = US
emailAddress            = hi@vectorized.io
organizationName        = Redpanda
organizationalUnitName  = Redpanda Test

[ distinguished_name ]
organizationName = Redpanda
commonName       = Redpanda Test CA

[ extensions ]
keyUsage         = critical,digitalSignature,nonRepudiation,keyEncipherment,keyCertSign,cRLSign
basicConstraints = critical,CA:true,pathlen:1

# Common policy for nodes and users.
[ signing_policy ]
organizationName = supplied
commonName       = optional

# Used to sign node certificates.
[ signing_node_req ]
keyUsage         = critical,digitalSignature,keyEncipherment,cRLSign
extendedKeyUsage = serverAuth,clientAuth

# Used to sign client certificates.
[ signing_client_req ]
keyUsage         = critical,digitalSignature,keyEncipherment,cRLSign
extendedKeyUsage = clientAuth
"""

# Culled from https://pki-tutorial.readthedocs.io/en/latest/simple/index.html

_root_ca_config_tmpl = """
# Redpanda Root CA

# The [default] section contains global constants that can be referred to from
# the entire configuration file. It may also hold settings pertaining to more
# than one openssl command.

[ default ]
ca                      = {name}                    # CA name
dir                     = {dir}                     # Top dir

# The next part of the configuration file is used by the openssl req command.
# It defines the CA's key pair, its DN, and the desired extensions for the CA
# certificate.

[ req ]
default_bits            = 2048                  # RSA key size
encrypt_key             = yes                   # Protect private key
default_md              = sha256                # MD to use
utf8                    = yes                   # Input is UTF-8
string_mask             = utf8only              # Emit UTF-8 strings
prompt                  = no                    # Don't prompt for DN
distinguished_name      = ca_dn                 # DN section
req_extensions          = ca_reqext             # Desired extensions

[ ca_dn ]
0.domainComponent       = "org"
1.domainComponent       = "simple"
organizationName        = "Redpanda"
organizationalUnitName  = "Redpanda Root CA"
commonName              = "{name}"

[ ca_reqext ]
keyUsage                = critical,keyCertSign,cRLSign
basicConstraints        = critical,CA:true
subjectKeyIdentifier    = hash

# The remainder of the configuration file is used by the openssl ca command.
# The CA section defines the locations of CA assets, as well as the policies
# applying to the CA.

[ ca ]
default_ca              = root_ca               # The default CA section

[ root_ca ]
certificate             = $dir/ca/$ca.crt       # The CA cert
private_key             = $dir/ca/$ca/private/$ca.key # CA private key
new_certs_dir           = $dir/ca/$ca           # Certificate archive
serial                  = $dir/ca/$ca/db/$ca.crt.srl # Serial number file
crlnumber               = $dir/ca/$ca/db/$ca.crl.srl # CRL number file
database                = $dir/ca/$ca/db/$ca.db # Index file
unique_subject          = no                    # Require unique subject
default_days            = 30                    # How long to certify for
default_md              = sha256                # MD to use
policy                  = match_pol             # Default naming policy
email_in_dn             = no                    # Add email to cert DN
preserve                = no                    # Keep passed DN ordering
name_opt                = ca_default            # Subject DN display options
cert_opt                = ca_default            # Certificate display options
copy_extensions         = none                  # Copy extensions from CSR
x509_extensions         = signing_ca_ext        # Default cert extensions
default_crl_days        = 365                   # How long before next CRL
crl_extensions          = crl_ext               # CRL extensions

# Naming policies control which parts of a DN end up in the certificate and
# under what circumstances certification should be denied.

[ match_pol ]
organizationName        = match                 # Must match 'Redpanda'
commonName              = supplied              # Must be present

# Certificate extensions define what types of certificates the CA is able to
# create.

[ root_ca_ext ]
keyUsage                = critical,keyCertSign,cRLSign
basicConstraints        = critical,CA:true
subjectKeyIdentifier    = hash
authorityKeyIdentifier  = keyid:always

[ signing_ca_ext ]
keyUsage                = critical,keyCertSign,cRLSign
basicConstraints        = critical,CA:true
subjectKeyIdentifier    = hash
authorityKeyIdentifier  = keyid:always

# CRL extensions exist solely to point to the CA certificate that has issued
# the CRL.

[ crl_ext ]
authorityKeyIdentifier  = keyid:always
"""

_signing_ca_config_tmpl = """
# Redpanda Signing CA

# The [default] section contains global constants that can be referred to from
# the entire configuration file. It may also hold settings pertaining to more
# than one openssl command.

[ default ]
ca                      = {name}                    # CA name
dir                     = {dir}                     # Top dir

# The next part of the configuration file is used by the openssl req command.
# It defines the CA's key pair, its DN, and the desired extensions for the CA
# certificate.

[ req ]
default_bits            = 2048                  # RSA key size
encrypt_key             = yes                   # Protect private key
default_md              = sha256                  # MD to use
utf8                    = yes                   # Input is UTF-8
string_mask             = utf8only              # Emit UTF-8 strings
prompt                  = no                    # Don't prompt for DN
distinguished_name      = ca_dn                 # DN section
req_extensions          = ca_reqext             # Desired extensions

[ ca_dn ]
organizationName        = "Redpanda"
organizationalUnitName  = "Redpanda Signing CA"
commonName              = "{name}"

[ ca_reqext ]
keyUsage                = critical,keyCertSign,cRLSign
basicConstraints        = critical,CA:true,pathlen:0
subjectKeyIdentifier    = hash

# The remainder of the configuration file is used by the openssl ca command.
# The CA section defines the locations of CA assets, as well as the policies
# applying to the CA.

[ ca ]
default_ca              = signing_ca            # The default CA section

[ signing_ca ]
certificate             = $dir/ca/$ca.crt       # The CA cert
private_key             = $dir/ca/$ca/private/$ca.key # CA private key
new_certs_dir           = $dir/ca/$ca           # Certificate archive
serial                  = $dir/ca/$ca/db/$ca.crt.srl # Serial number file
crlnumber               = $dir/ca/$ca/db/$ca.crl.srl # CRL number file
database                = $dir/ca/$ca/db/$ca.db # Index file
unique_subject          = no                    # Require unique subject
default_days            = 7                     # How long to certify for
default_md              = sha256                  # MD to use
policy                  = match_pol             # Default naming policy
email_in_dn             = no                    # Add email to cert DN
preserve                = no                    # Keep passed DN ordering
name_opt                = ca_default            # Subject DN display options
cert_opt                = ca_default            # Certificate display options
copy_extensions         = copy                  # Copy extensions from CSR
x509_extensions         = server_ext             # Default cert extensions
default_crl_days        = 7                     # How long before next CRL
crl_extensions          = crl_ext               # CRL extensions

# Naming policies control which parts of a DN end up in the certificate and
# under what circumstances certification should be denied.

[ match_pol ]
organizationName        = match                 # Must match 'Redpanda'
commonName              = supplied              # Must be present

# Certificate extensions define what types of certificates the CA is able to
# create.

[ server_ext ]
keyUsage                = critical,digitalSignature,keyEncipherment
basicConstraints        = CA:false
extendedKeyUsage        = serverAuth,clientAuth
subjectKeyIdentifier    = hash
authorityKeyIdentifier  = keyid:always

[ signing_ca_ext ]
keyUsage                = critical,keyCertSign,cRLSign
basicConstraints        = critical,CA:true
subjectKeyIdentifier    = hash
authorityKeyIdentifier  = keyid:always

# CRL extensions exist solely to point to the CA certificate that has issued
# the CRL.

[ crl_ext ]
authorityKeyIdentifier  = keyid:always
"""

_node_config_tmpl = """
# OpenSSL node configuration file
[ req ]
prompt=no
distinguished_name = distinguished_name
req_extensions = extensions

[ distinguished_name ]
organizationName = Redpanda
{common_name}

[ extensions ]
subjectAltName = critical,DNS:{host}
"""

_server_config_tmpl = """
# TLS server certificate request

# This file is used by the openssl req command. The subjectAltName cannot be
# prompted for and must be specified in the SAN environment variable.

[ default ]
SAN                     = DNS:yourdomain.tld    # Default value

[ req ]
default_bits            = 2048                  # RSA key size
encrypt_key             = no                    # Protect private key
default_md              = sha256                # MD to use
utf8                    = yes                   # Input is UTF-8
string_mask             = utf8only              # Emit UTF-8 strings
prompt                  = no                    # Prompt for DN
distinguished_name      = server_dn             # DN template
req_extensions          = server_reqext         # Desired extensions

[ server_dn ]
organizationName = Redpanda
{common_name}

[ server_reqext ]
subjectAltName          = critical,DNS:{host}
keyUsage                = critical,digitalSignature,keyEncipherment
basicConstraints        = CA:false
extendedKeyUsage        = serverAuth,clientAuth
subjectKeyIdentifier    = hash
"""

CertificateAuthority = collections.namedtuple("CertificateAuthority",
                                              ["cfg", "key", "crt", "crl"])
Certificate = collections.namedtuple(
    "Certificate", ["cfg", "key", "crt", "ca", "p12_file", "p12_password"])


class TLSCertManager:
    """
    When a TLSCertManager is instantiated a new CA is automatically created and
    certificates can be created immediately.

    All of the generated files (keys, certs, etc...) are stored in a temporary
    directory that will be deleted when the TLSCertManager is destroyed. Since
    it is common for clients to take paths to these files, it is best to keep
    the instance alive for as long as the files are in use.
    """
    def __init__(self, logger, ca_end_date=None, cert_expiry_days=1):
        self._logger = logger
        self._dir = tempfile.TemporaryDirectory()
        self._ca_end_date = ca_end_date
        self._cert_expiry_days = cert_expiry_days
        self._ca = self._create_ca()
        self.certs: dict[str, Certificate] = {}

    @staticmethod
    def generate_password(
            length: int,
            choices: str = string.ascii_letters + string.digits) -> str:
        return ''.join(random.choices(choices, k=length))

    def _with_dir(self, *args):
        return os.path.join(self._dir.name, *args)

    def _exec(self, cmd):
        self._logger.info(f"Running command: {cmd}")
        retries = 0
        output = None
        while retries < 3:
            try:
                output = subprocess.check_output(cmd.split(),
                                                 cwd=self._dir.name,
                                                 stderr=subprocess.STDOUT)
                retries = 3  # Stop retry
            except subprocess.CalledProcessError as e:
                self._logger.error(f"openssl error: {e.output}")
                output = subprocess.check_output(
                    ["df", "--human-readable", self._dir.name],
                    cwd=self._dir.name,
                    stderr=subprocess.STDOUT)
                self._logger.error(f"disk space on {self._dir.name} {output}")

                if retries >= 3:
                    raise
            else:
                self._logger.debug(output)

            retries += 1

    def _create_ca(self) -> CertificateAuthority:
        cfg = self._with_dir("ca.conf")
        key = self._with_dir("ca.key")
        crt = self._with_dir("ca.crt")
        idx = self._with_dir("index.txt")
        srl = self._with_dir("serial.txt")
        crl_srl = self._with_dir("crlnumber.txt")

        with open(f"{cfg}", "w") as f:
            f.write(_ca_config_tmpl.format(dir=self._dir.name))

        self._exec(f"openssl genrsa -out {key} 2048")
        self._exec(f"openssl req -new -x509 -config {cfg} "
                   f"-key {key} -out {crt} -days 365 -batch")

        if os.path.exists(idx): os.remove(idx)
        if os.path.exists(srl): os.remove(srl)
        pathlib.Path(idx).touch()
        with open(srl, "w") as f:
            f.writelines(["01"])

        crl = self._create_crl("ca", cfg, crl_srl)

        return CertificateAuthority(cfg, key, crt, crl)

    def _create_crl(self, ca: str, cfg: str, crl_srl: str) -> str:
        if os.path.exists(crl_srl): os.remove(crl_srl)
        with open(crl_srl, 'w') as f:
            f.writelines(["01"])

        crls = self._with_dir('crl')
        os.makedirs(crls, exist_ok=True)
        crl = os.path.join(crls, f"{ca}.crl")

        self._exec(f"openssl ca -gencrl -config {cfg} -out {crl}")

        return crl

    @property
    def ca(self) -> CertificateAuthority:
        return self._ca

    def create_cert(self,
                    host: str,
                    *,
                    common_name: typing.Optional[str] = None,
                    name: typing.Optional[str] = None,
                    faketime: typing.Optional[str] = '-0d') -> Certificate:
        name = name or host

        cfg = self._with_dir(f"{name}.conf")
        key = self._with_dir(f"{name}.key")
        csr = self._with_dir(f"{name}.csr")
        crt = self._with_dir(f"{name}.crt")
        p12_file = self._with_dir(f"{name}.p12")

        with open(cfg, "w") as f:
            if common_name is None:
                common_name = ""
            else:
                common_name = f"commonName = {common_name}"
            f.write(
                _node_config_tmpl.format(host=host, common_name=common_name))

        self._exec(f"openssl genrsa -out {key} 2048")

        self._exec(f"openssl req -new -config {cfg} "
                   f"-key {key} -out {csr} -batch")

        self._exec(
            f"faketime -f {faketime} openssl ca -config {self.ca.cfg} -keyfile "
            f"{self.ca.key} -cert {self.ca.crt} -policy signing_policy "
            f"-extensions signing_node_req -in {csr} -out {crt} "
            f"-days {self._cert_expiry_days} -outdir {self._dir.name} -batch")

        p12_password = self.generate_pkcs12_file(p12_file, key, crt,
                                                 self.ca.crt)

        cert = Certificate(cfg, key, crt, self.ca, p12_file, p12_password)
        self.certs[name] = cert
        return cert

    def generate_pkcs12_file(self,
                             p12_file: str,
                             key: str,
                             crt: str,
                             ca: str,
                             pw_length: int = 16) -> str:
        """
        Runs command to generate a new PKCS#12 file and returns the generated password
        """
        p12_password = self.generate_password(length=pw_length)
        self._exec(
            f'openssl pkcs12 -export -inkey {key} -in {crt} -certfile {ca} -passout pass:{p12_password} -out {p12_file}'
        )
        return p12_password

    # TODO(oren): reasons enum
    def revoke_cert(self, crt: Certificate, reason: str = "unspecified"):
        self._exec(
            f"openssl ca -config {crt.ca.cfg} -revoke {crt.crt} -crl_reason {reason}"
        )
        crl_srl = self._with_dir("crlnumber.txt")
        crl = self._create_crl("ca", crt.ca.cfg, crl_srl)
        revoked = self._with_dir("revoked.txt")
        self._exec(
            f'openssl crl -inform PEM -text -noout -in {crl} -out {revoked}')
        with open(revoked, 'r') as f:
            self._logger.debug(f"\n{f.read()}")


class TLSChainCACertManager(TLSCertManager):
    """
    Similar to TLSCertManager, but generates a chain of CAs,
    starting with a self-signed root CA and followed by signing
    CAs of incrementally decreasing lifetime.

    The length of the CA chain, CA expiry increment, and signed
    cert expiry are all configurable (in days) at the constructor.

    Should drop in cleanly for TLSCertManager.
    """
    DEFAULT_EXPIRY_DAYS = 365

    def __init__(self,
                 logger,
                 chain_len=2,
                 cert_expiry_days=1,
                 ca_expiry_days=7):
        assert chain_len > 0
        self._logger = logger
        self._dir = tempfile.TemporaryDirectory()
        self.cert_expiry_days = cert_expiry_days
        self.ca_expiry_days = ca_expiry_days
        self._cas: list[CertificateAuthority] = []
        self._cas.append(
            self._create_ca(
                'root-ca',
                _root_ca_config_tmpl,
                days=chain_len * self.ca_expiry_days,
            ))
        for i in range(1, chain_len):
            self._cas.append(
                self._create_ca(
                    f'signing-ca-{i-1}',
                    _signing_ca_config_tmpl,
                    days=(chain_len - i) * ca_expiry_days,
                    parent_cfg=self._cas[-1].cfg,
                    ext='signing_ca_ext',
                ))
        self._cert_chain = self._create_ca_cert_chain()
        self.certs: dict[str, Certificate] = {}

    @property
    def ca(self) -> CertificateAuthority:
        return self._cert_chain

    @property
    def signing_ca(self) -> CertificateAuthority:
        return self._cas[-1]

    def _create_ca(self,
                   ca: str,
                   tmpl: str,
                   days: int = DEFAULT_EXPIRY_DAYS,
                   parent_cfg: typing.Optional[str] = None,
                   ext: str = 'root_ca_ext') -> CertificateAuthority:
        dir = self._with_dir('ca')
        pvt = self._with_dir('ca', ca, 'private')
        db = self._with_dir('ca', ca, 'db')
        certs = self._with_dir('certs')

        cfg = self._with_dir(f"{ca}.conf")
        selfsign = parent_cfg is None
        parent_cfg = cfg if selfsign else self._with_dir(parent_cfg)
        key = os.path.join(pvt, f"{ca}.key")
        csr = os.path.join(dir, f"{ca}.csr")
        crt = os.path.join(dir, f"{ca}.crt")
        idx = os.path.join(db, f"{ca}.db")
        srl = os.path.join(db, f"{ca}.crt.srl")
        crl_srl = os.path.join(db, f"{ca}.crl.srl")

        with open(f"{cfg}", "w") as f:
            f.write(tmpl.format(dir=self._dir.name, name=ca))

        os.makedirs(pvt, mode=0o700, exist_ok=True)
        os.makedirs(db, exist_ok=True)
        os.makedirs(certs, exist_ok=True)

        if os.path.exists(idx): os.remove(idx)
        if os.path.exists(srl): os.remove(srl)
        pathlib.Path(idx).touch()
        pathlib.Path(idx + ".attr").touch()
        with open(srl, 'w') as f:
            f.writelines(["01"])

        self._exec(
            f"openssl req -new -nodes -config {cfg} -out {csr} -keyout {key}")
        self._exec(
            f"openssl ca {'-selfsign' if selfsign else ''} -config {parent_cfg} "
            f"-in {csr} -out {crt} -extensions {ext} -days {days} -batch")

        crl = self._create_crl(ca, cfg, crl_srl)

        return CertificateAuthority(cfg, key, crt, crl)

    def _create_ca_cert_chain(self) -> CertificateAuthority:
        # First create the signing ca chain
        ca_files = [ca.crt for ca in self._cas]
        out = self._with_dir('ca', 'signing-ca-chain.pem')
        pathlib.Path(out).touch()
        with open(out, 'w') as outfile:
            for fname in reversed(ca_files):
                with open(fname, 'r') as infile:
                    outfile.write(infile.read())

        with open(out, 'r') as f:
            self._logger.debug(f"CA chain: {f.read()}")

        # Now do the same for the CRLs
        crl_files = [ca.crl for ca in self._cas]
        crl_out = self._with_dir('ca', 'signing-crl-chain.crl')
        pathlib.Path(crl_out).touch()
        with open(crl_out, 'w') as outfile:
            for fname in reversed(crl_files):
                with open(fname, 'r') as infile:
                    outfile.write(infile.read())

        with open(crl_out, 'r') as f:
            self._logger.debug(f"CRL chain: {f.read()}")

        return CertificateAuthority(None, None, out, crl_out)

    def create_cert(self,
                    host: str,
                    *,
                    common_name: typing.Optional[str] = None,
                    name: typing.Optional[str] = None,
                    faketime: typing.Optional[str] = '-0d') -> Certificate:
        name = name or host

        cfg = self._with_dir(f"{name}.conf")
        key = self._with_dir('certs', f"{name}.key")
        csr = self._with_dir('certs', f"{name}.csr")
        crt = self._with_dir('certs', f"{name}.crt")
        p12_file = self._with_dir('certs', f"{name}.p12")

        with open(cfg, "w") as f:
            if common_name is None:
                common_name = f"commonName = {name}"
            else:
                common_name = f"commonName = {common_name}"
            f.write(
                _server_config_tmpl.format(host=host, common_name=common_name))

        self._exec(f"openssl req -new -nodes -config {cfg} "
                   f"-keyout {key} -out {csr} -batch")
        self._exec(
            f"faketime -f {faketime} openssl ca -config {self.signing_ca.cfg} -policy match_pol "
            f"-in {csr} -out {crt} -extensions server_ext -days {self.cert_expiry_days} -batch"
        )

        p12_password = self.generate_pkcs12_file(p12_file, key, crt,
                                                 self.ca.crt)

        cert = Certificate(cfg, key, crt, self.ca, p12_file, p12_password)
        self.certs[name] = cert
        return cert
