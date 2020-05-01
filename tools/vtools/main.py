import click
import os

from absl import logging

logging._warn_preinit_stderr = 0

from .clean import commands as clean
from .fmt import commands as fmt
from .git import commands as git
from .customer import commands as customer
from .ssh import commands as ssh
from .deploy import commands as deploy
from .install import commands as install
from .build import commands as build
from .test import commands as test
from .publish import commands as publish
from .dbuild import commands as dbuild
from .storage import commands as storage
from .ci import commands as ci
from .punisher import commands as punisher
from .cluster import commands as cluster

logging.use_absl_handler()
logging.set_verbosity(logging.INFO)

if os.environ.get("VTOOLS_LOG_LEVEL", None):
    lvl = os.environ.get("VTOOLS_LOG_LEVEL")
    lvl = getattr(logging, lvl.upper())
    logging.set_verbosity(lvl)
elif os.environ.get('CI', None):
    logging.set_verbosity(logging.DEBUG)


@click.group()
def main():
    # entrypoint
    pass


# add commands here
main.add_command(clean.clean)
main.add_command(fmt.fmt)
main.add_command(git.git)
main.add_command(customer.customer)
main.add_command(ssh.ssh)
main.add_command(deploy.deploy)
main.add_command(install.install)
main.add_command(build.build)
main.add_command(test.test)
main.add_command(publish.publish)
main.add_command(dbuild.dbuild)
main.add_command(storage.storage)
main.add_command(ci.ci)
main.add_command(punisher.punisher)
main.add_command(cluster.cluster)
