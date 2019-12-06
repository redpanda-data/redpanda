import click
from ..vlib import packagecloud as pc


@click.group()
def client():
    pass


@client.command()
@click.option(
    '--access-token',
    help="Your Packagecloud access token. https://packagecloud.io/api_token")
@click.option('--client-name', help='The client company name')
@click.option(
    '--client-id',
    help=
    'A unique string to identify the client by. Used to create unique resources such as the package repo tokens, the docs bucket, etc.'
)
def onboard(access_token, client_name, client_id):
    token = pc.create_session_token(access_token)
    master_token = pc.create_master_token(token, client_id)
    click.echo(master_token)
    read_token = pc.create_read_token(token, master_token['id'], client_id)
