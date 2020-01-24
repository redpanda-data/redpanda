from datetime import date
import click
from ..vlib import packagecloud as pc
from ..vlib import templates as tmpl
import os


@click.group(short_help='onboard/offboard customers.')
def customer():
    pass


@customer.command(short_help='add a new customer to packagecloud')
@click.option(
    '--access-token',
    help="Your Packagecloud access token. https://packagecloud.io/api_token")
@click.option('--customer-name', help='Name of company being onboarded.')
@click.option(
    '--customer-id',
    help=('A unique string to identify the customer by. Used to create unique'
          'resources such as the package repo tokens, the docs bucket, etc.'))
@click.option('--docs-out-dir',
              default='.',
              help='The directory where the rendered docs will be output')
def onboard(access_token, customer_name, customer_id, docs_out_dir):
    token = pc.create_session_token(access_token)
    master_token = pc.create_master_token(token, customer_id)
    click.echo(master_token)
    pc.create_read_token(token, master_token['id'], customer_id)
    tmpl.render_to_file(
        f'{os.path.dirname(__file__)}/docs-template.md',
        f'{docs_out_dir}/docs.md', {
            'customer_name': customer_name,
            'master_token': master_token,
            'date': date.today()
        })


@customer.command(short_help='remove a customer from packagecloud')
@click.option(
    '--access-token',
    help="Your Packagecloud access token. https://packagecloud.io/api_token")
@click.option('--token-id',
              help="The integer ID of the master token to revoke")
def offboard(access_token, token_id):
    token = pc.create_session_token(access_token)
    info = pc.show_master_token(token, token_id)
    click.echo(
        "You're about to revoke this token and its associated resources:")
    click.echo(info)
    if click.confirm('Are you sure you want to continue?'):
        pc.revoke_master_token(token, token_id)
