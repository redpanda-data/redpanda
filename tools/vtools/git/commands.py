import click
from git import Repo


@click.command()
@click.option("--path", default=".", help="path to git repo")
def verify_git(path):
    """verify the git user name and password end in vectorized.io"""
    r = Repo(path, search_parent_directories=True)
    reader = r.config_reader()
    email = reader.get_value("user", "email")
    name = reader.get_value("user", "name")
    sendmail_smtp = reader.get_value("sendemail", "smtpuser")
    for mail in [email, sendmail_smtp]:
        if not mail.endswith("@vectorized.io"):
            raise click.ClickException(
                f'Invalid email({email}), use @vectorized.io')
    git_root = r.git.rev_parse("--show-toplevel")
    click.echo(f"valid repo settings for: {git_root}")
