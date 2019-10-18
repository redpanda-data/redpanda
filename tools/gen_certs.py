#!/usr/bin/env python3

import logging
import sys
import os
import socket
from shutil import copyfile

import log
import shell

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

DEFAULT_CERTIFICATE_AUTHORITY = "root_certificate_authority"


def gen_key(name):
    logger.info("Generating key: %s", name)
    cmd = "openssl genrsa -out %s.key 2048" % name
    shell.run_subprocess(cmd)


def get_ip():
    # doesn't even have to be reachable
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


def gen_csr_subject():
    return "/C=US/ST=SanFrancisco/L=California/O=Vectorized/OU=Dev/CN=%s" % get_ip(
    )


def gen_csr_dname():
    return "C=US ST=SanFrancisco L=California O=Vectorized OU=Dev CN=%s" % get_ip(
    )


def gen_csr(name):
    logger.info("Generating csr for: %s", name)
    cmd = "openssl req -new -key {name}.key -newkey rsa:2048 -out {name}.csr -sha256 -subj '{subject}'".format(
        name=name, subject=gen_csr_subject())
    shell.run_subprocess(cmd)
    verifier_csr_cmd = "openssl req -in {name}.csr -noout -text".format(
        name=name)
    shell.run_subprocess(verifier_csr_cmd)


def gen_self_sign_cert(ca, name):
    logger.info("Self signing cert for ca: %s, and csr: %s", ca, name)
    cmd = "openssl x509 -req -days 3650 -in {name}.csr -CA {ca}.pem -CAkey" \
        " {ca}.key -CAcreateserial -out {name}.crt -sha256".format(name=name, ca=ca)
    shell.run_subprocess(cmd)


def gen_pem(name):
    logger.info("Generating pem")
    cmd ="openssl req -x509 -days 3650 -new -nodes -key {name}.key -newkey rsa:2048"\
        " -sha256 -out {name}.pem -subj '{subject}'".format(name=name, subject=gen_csr_subject())
    shell.run_subprocess(cmd)
    copyfile("%s.pem" % name, "%s.chain_cert" % name)


def gen_ca_cert(name):
    if os.path.exists("%s.key" % name): return
    logger.info('Generating ca:%s', name)
    gen_key(name)
    gen_pem(name)


def gen_certs(name, ca):
    gen_ca_cert(ca)
    gen_key(name)
    gen_csr(name)
    gen_self_sign_cert(ca, name)


def main():
    import argparse

    def _generate_options():
        parser = argparse.ArgumentParser(
            description='Generate set of self signed certificates')
        parser.add_argument(
            '--log',
            type=str.lower,
            default='info',
            choices=['critical', 'error', 'warning', 'info', 'debug'],
            help=
            'log level, one of ' \
            '[critical, error, warning, info, debug] i.e: --log debug'
        )
        parser.add_argument(
            '--name',
            type=str,
            default='redpanda',
            help='Certificate/key name')
        return parser

    parser = _generate_options()
    options = parser.parse_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info(options)

    gen_certs(options.name, DEFAULT_CERTIFICATE_AUTHORITY)


if __name__ == '__main__':
    main()