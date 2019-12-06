import click


from .pkg import commands as pkg
from .git import commands as git
from .client import commands as client

@click.group()
def main():
    #entry point
    pass

# add commands here
main.add_command(pkg.print_deps)
main.add_command(git.verify_git)
main.add_command(client.client)
