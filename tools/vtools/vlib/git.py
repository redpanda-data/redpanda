import click
import git
from absl import logging

from configparser import NoSectionError


def read_git_value(reader, config_section, config_name):
    """returns the git config value for config_name under config_section

    If the value doesn't exist an exception is raised that will cause the
    verification to exit with an error status.
    """
    try:
        return reader.get_value(config_section, config_name)
    except NoSectionError:
        raise click.ClickException(
            f'failed to read {config_name} config under the {config_section} section (check your .git/config)'
        )


def verify(path):
    """verify the git user name and password end in vectorized.io"""
    r = git.Repo(path, search_parent_directories=True)
    reader = r.config_reader()
    email = read_git_value(reader, "user", "email")
    sendmail_smtp = read_git_value(reader, "sendemail", "smtpuser")

    for mail in [email, sendmail_smtp]:
        if not mail.endswith("@vectorized.io"):
            raise click.ClickException(
                f'Invalid email({email}), use @vectorized.io')
    git_root = r.git.rev_parse("--show-toplevel")
    logging.info(f"valid repo settings for: {git_root}")


def get_email(path):
    r = git.Repo(path, search_parent_directories=True)
    reader = r.config_reader()
    return read_git_value(reader, "user", "email")
