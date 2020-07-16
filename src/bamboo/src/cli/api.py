import random

# 3rd party
import click

# ours
from ..localsession import docker_session


@click.group(short_help='session command line interface')
def session():
    pass


@session.command(short_help='create new session')
@click.option('--sname', help="name of the session", default=None)
def init(sname):
    docker_session.generate_docker_session(sname)


@session.command(short_help='add new node to session')
@click.option('--sname', help="name of the session")
@click.option('--node-id', help="id of node", default=None)
@click.option('--package', help="id of node")
def add_node(sname, node_id, package):
    if node_id is None:
        node_id = random.randint(1, 2**31)
    s = docker_session.generate_docker_session(sname)
    s.add_node(node_id, package)
