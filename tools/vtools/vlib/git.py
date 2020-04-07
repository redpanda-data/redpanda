import click
import git
from absl import logging


def verify(path):
    """verify the git user name and password end in vectorized.io"""
    r = git.Repo(path, search_parent_directories=True)
    reader = r.config_reader()
    email = reader.get_value("user", "email")
    sendmail_smtp = reader.get_value("sendemail", "smtpuser")
    for mail in [email, sendmail_smtp]:
        if not mail.endswith("@vectorized.io"):
            raise click.ClickException(
                f'Invalid email({email}), use @vectorized.io')
    return r.git.rev_parse("--show-toplevel")


def get_email(path):
    r = git.Repo(path, search_parent_directories=True)
    reader = r.config_reader()
    return reader.get_value("user", "email")
