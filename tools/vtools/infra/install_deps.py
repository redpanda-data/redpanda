import argparse
import os
import shutil
import sys
from urllib import request
from absl import logging
from ..vlib import shell


def check_deps_installed(vconfig):
    return _check_installed(
        vconfig.infra_bin_dir, 'terraform') and _check_installed(
            vconfig.infra_bin_dir, os.path.join('v2', 'current', 'bin', 'aws'))


def install_deps(vconfig):
    os.makedirs(vconfig.infra_bin_dir, exist_ok=True)
    _install_awscli(vconfig.infra_bin_dir)
    _install_terraform(vconfig.infra_bin_dir)


def _install_awscli(install_dir):
    awscli_zip = os.path.join(install_dir, 'awscliv2.zip')
    awscli_url = 'https://d1vvhvl2y92vvt.cloudfront.net/awscli-exe-linux-x86_64.zip'
    install_cmd = f"""{os.path.join(install_dir, 'aws', 'install')} \
    --install-dir {install_dir} \
    --bin-dir {install_dir}"""
    logging.info('Downloading AWS CLI...')
    _download_and_extract(awscli_url, awscli_zip, install_dir)
    installed = os.path.isfile(
        os.path.join(install_dir, 'v2', 'current', 'bin', 'aws'))
    if installed:
        logging.info('Found existing AWS CLI installation. Updating...')
        shell.run_subprocess(f'{install_cmd} --update')
    else:
        shell.run_subprocess(install_cmd)
        shell.run_subprocess(f'{os.path.join(install_dir, "aws")} configure')
    shutil.rmtree(os.path.join(install_dir, 'aws'))


def _install_terraform(install_dir):
    tf_zip = os.path.join(install_dir, 'terraform.zip')
    tf_url = 'https://releases.hashicorp.com/terraform/0.12.15/terraform_0.12.15_linux_amd64.zip'
    logging.info('Downloading Terraform...')
    _download_and_extract(tf_url, tf_zip, install_dir)


def _check_installed(install_dir, name):
    return os.path.isfile(os.path.join(install_dir, name))


def _download_and_extract(url, dest, extract_to):
    with request.urlopen(url) as res, open(dest, 'wb') as out:
        shutil.copyfileobj(res, out)
    # The exctraction has to be done with unzip because ZipFile has a bug and
    # it does not preserve file permissions.
    # See https://bugs.python.org/issue15795
    shell.run_subprocess(f'unzip -o -d {extract_to} {dest}')
    os.remove(dest)
