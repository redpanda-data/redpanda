"""
This module contains functions to generate a simple CA
"""

load("@bazel_skylib//rules:run_binary.bzl", "run_binary")

openssl_env = {
    "OPENSSL_CONF": "$(execpath @openssl//:openssl_data)/openssl.cnf",
}

# buildifier: disable=function-docstring-args
def _redpanda_private_key(name, certificate):
    private_key = certificate + ".key"

    run_binary(
        name = name + "_key_gen",
        srcs = [
            "@openssl//:openssl_data",
        ],
        outs = [private_key],
        args = [
            "ecparam",
            "-name",
            "prime256v1",
            "-genkey",
            "-noout",
            "-out",
            "$(execpath :{})".format(private_key),
        ],
        env = openssl_env,
        tool = "@openssl//:openssl_exe",
    )

    return private_key

def redpanda_selfsigned_cert(name, certificate, common_name, visibility = None):
    """
    Generate a Redpanda self-signed certificate.

    Args:
      name: name of the target
      certificate: name to use for output files (crt, key, and csr)
      common_name: the CN to use when setting the subject name
      visibility: visibility setting
    """

    cert = certificate + ".crt"
    subj = "/C=US/ST=California/L=San Francisco/O=Redpanda Data/OU=Core/CN=" + common_name

    private_key = _redpanda_private_key(name, certificate)

    run_binary(
        name = name + "_crt_gen",
        srcs = [
            private_key,
            "@openssl//:openssl_data",
        ],
        outs = [cert],
        args = [
            "req",
            "-new",
            "-x509",
            "-sha256",
            "-key",
            "$(execpath :{})".format(private_key),
            "-out",
            "$(execpath :{})".format(cert),
            "-subj",
            subj,
            "-addext",
            "subjectAltName = IP:127.0.0.1",
        ],
        env = openssl_env,
        tool = "@openssl//:openssl_exe",
    )

    native.filegroup(
        name = name,
        srcs = [private_key, cert],
        visibility = visibility,
    )

def redpanda_signed_cert(name, certificate, common_name, ca, serial_number, visibility = None):
    """
    Generate a Redpanda signed certificate.

    Args:
      name: name of the target
      certificate: name to use for output files (crt, key, and csr)
      common_name: the CN to use when setting the subject name
      ca: the certificate to be used as the signing CA
      serial_number: the serial number of cert when issued by CA
      visibility: visibility setting
    """

    subj = "/C=US/ST=California/L=San Francisco/O=Redpanda Data/OU=Core/CN=" + common_name

    private_key = _redpanda_private_key(name, certificate)
    csr = certificate + ".csr"
    cert = certificate + ".crt"

    run_binary(
        name = name + "_csr_gen",
        srcs = [
            private_key,
            "@openssl//:openssl_data",
        ],
        outs = [csr],
        args = [
            "req",
            "-new",
            "-sha256",
            "-key",
            "$(execpath :{})".format(private_key),
            "-out",
            "$(execpath :{})".format(csr),
            "-subj",
            subj,
        ],
        env = openssl_env,
        tool = "@openssl//:openssl_exe",
    )

    ca_cert = ca + ".crt"
    ca_private_key = ca + ".key"

    run_binary(
        name = name + "_crt_gen",
        srcs = [
            ca_cert,
            ca_private_key,
            csr,
            "@openssl//:openssl_data",
        ],
        outs = [cert],
        args = [
            "x509",
            "-req",
            "-days",
            "1000",
            "-sha256",
            "-set_serial",
            "{}".format(serial_number),
            "-in",
            "$(execpath :{})".format(csr),
            "-CA",
            "$(execpaths :{})".format(ca_cert),
            "-CAkey",
            "$(execpaths :{})".format(ca_private_key),
            "-out",
            "$(execpath :{})".format(cert),
        ],
        env = openssl_env,
        tool = "@openssl//:openssl_exe",
    )

    native.filegroup(
        name = name,
        srcs = [private_key, csr, cert],
        visibility = visibility,
    )
