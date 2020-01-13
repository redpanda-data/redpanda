import click
import os

from absl import logging

logging._warn_preinit_stderr = 0

from .clean import commands as clean
from .git import commands as git
from .customer import commands as customer
from .ssh import commands as ssh
from .infra import commands as infra
from .install import commands as install
from .build import commands as build
from .test import commands as test

logging.use_absl_handler()
logging.set_verbosity(logging.INFO)

if os.environ.get('CI', None):
    logging.set_verbosity(logging.DEBUG)


@click.group()
def main():
    # entrypoint
    pass


# add commands here
main.add_command(clean.clean)
main.add_command(git.git)
main.add_command(customer.customer)
main.add_command(ssh.ssh)
main.add_command(infra.infra)
main.add_command(install.install)
main.add_command(build.build)
main.add_command(test.test)
