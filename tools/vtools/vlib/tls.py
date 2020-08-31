from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
import datetime
import uuid
import jks
import os
import ipaddress

ONE_DAY = datetime.timedelta(1, 0, 0)


class SecretsProvider:
    def __init__(self, logger):
        self._logger = logger
        self._generate_ca_cert()

    def _generate_ca_cert(self):
        self._logger.info("generating test Certificate Authority")
        self._ca_private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend())

        self._ca_public_key = self._ca_private_key.public_key()

        builder = x509.CertificateBuilder()
        subj = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u'Redpanda Test CA'),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, u'vectorized'),
            x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME,
                               u'ducktape-test-harness'),
        ])
        builder = builder.subject_name(subj)
        builder = builder.issuer_name(subj)

        builder = builder.not_valid_before(datetime.datetime.today() - ONE_DAY)
        builder = builder.not_valid_after(datetime.datetime.today() + ONE_DAY)
        builder = builder.serial_number(x509.random_serial_number())
        builder = builder.public_key(self._ca_public_key)
        builder = builder.add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        self._ca_cert = builder.sign(private_key=self._ca_private_key,
                                     algorithm=hashes.SHA256(),
                                     backend=default_backend())

    def _generate_self_signed_cert(self, cn, private_key, public_key, ip=None):
        builder = x509.CertificateBuilder()
        builder = builder.subject_name(
            x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, u'{}'.format(cn)),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, u'vectorized'),
                x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME,
                                   u'ducktape-test-harness'),
            ]))

        builder = builder.issuer_name(self._ca_cert.issuer)
        builder = builder.not_valid_before(datetime.datetime.today() - ONE_DAY)
        builder = builder.not_valid_after(datetime.datetime.today() + ONE_DAY)
        builder = builder.serial_number(x509.random_serial_number())
        builder = builder.public_key(public_key)
        alt_names = [x509.DNSName(u"{}".format(cn))]
        if ip is not None:
            alt_names.append(x509.IPAddress(ipaddress.IPv4Address(ip)))

        builder = builder.add_extension(
            x509.SubjectAlternativeName(alt_names),
            critical=False,
        )
        return builder.sign(private_key=self._ca_private_key,
                            algorithm=hashes.SHA256(),
                            backend=default_backend())

    def generate_secrets(self, cn, ip=None):
        self._logger.debug("generating secrets for %s", cn)
        private_key = rsa.generate_private_key(public_exponent=65537,
                                               key_size=2048,
                                               backend=default_backend())
        public_key = private_key.public_key()
        cert = self._generate_self_signed_cert(cn=cn,
                                               private_key=private_key,
                                               public_key=public_key,
                                               ip=ip)

        return [private_key, public_key, cert]

    def encode_as_pem(self, key, cert):
        cert_pem = cert.public_bytes(encoding=serialization.Encoding.PEM)

        prv_key_pem = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return [prv_key_pem, cert_pem]

    def truststore_pem(self):
        return self._ca_cert.public_bytes(encoding=serialization.Encoding.PEM)

    def keystore_jks(self, cert, key, password):
        cert_der = cert.public_bytes(encoding=serialization.Encoding.DER)

        prv_key_der = key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        pke = jks.PrivateKeyEntry.new('node', [cert_der], prv_key_der, 'pkcs8')
        ks = jks.KeyStore.new('jks', [pke])
        return ks.saves(password)

    def truststore_jks(self, password):
        pke = jks.TrustedCertEntry.new('root', self.truststore_pem())
        ks = jks.KeyStore.new('jks', [pke])
        return ks.saves(password)

    def write_secrets_pem(self, cn, dir):
        [private_key, public_key, cert] = self.generate_secrets(cn)
        [key, cert] = self.encode_as_pem(private_key, cert)

        p = os.path.join(dir, "private_key.pem")
        with open(p, 'wb') as f:
            f.write(key)

        p = os.path.join(dir, "cert.pem")
        with open(p, 'wb') as f:
            f.write(cert)

        p = os.path.join(dir, "ca.pem")
        with open(p, 'wb') as f:
            f.write(self.truststore_pem())
