import click
import os

from absl import logging

logging._warn_preinit_stderr = 0

from .cli import api

logging.use_absl_handler()
logging.set_verbosity(logging.INFO)

if os.environ.get("BAMBOO_LOG_LEVEL", None):
    lvl = os.environ.get("BAMBOO_LOG_LEVEL")
    lvl = getattr(logging, lvl.upper())
    logging.set_verbosity(lvl)
elif os.environ.get('CI', None):
    logging.set_verbosity(logging.DEBUG)


@click.group()
def main():
    # entrypoint
    pass


# add commands here
main.add_command(api.session)
