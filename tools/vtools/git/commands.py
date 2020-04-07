import os
import re
import shutil
import subprocess
import tempfile
import click
import git as g
import sys
from configparser import NoSectionError
from absl import logging
from ..fmt import commands as fmt


@click.group(short_help='git niceties')
def git():
    pass


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


@git.command(short_help='verify git user and password end in vectorized.io')
@click.option("--path", default=".", help="path to git repo")
def verify(path):
    """verify the git user name and password end in vectorized.io"""
    r = g.Repo(path, search_parent_directories=True)
    reader = r.config_reader()
    email = read_git_value(reader, "user", "email")
    sendmail_smtp = read_git_value(reader, "sendemail", "smtpuser")

    for mail in [email, sendmail_smtp]:
        if not mail.endswith("@vectorized.io"):
            raise click.ClickException(
                f'Invalid email({email}), use @vectorized.io')
    git_root = r.git.rev_parse("--show-toplevel")
    logging.info(f"valid repo settings for: {git_root}")


@git.command(short_help='mildly opinionated patch submission.')
@click.option("-f",
              "--fork",
              default="origin",
              show_default=True,
              help="A remote for publishing the feature")
@click.option("-u",
              "--upstream",
              default=None,
              help="The upstream branch (e.g. upstream/master)")
@click.option("-t",
              "--to",
              default="v-dev@vectorized.io",
              show_default=True,
              help="Where to send patches")
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default='')
@click.option('--skip-fmt',
              help='Do not format files (fmt all).',
              is_flag=True)
@click.option('-s',
              '--suppress-cc',
              default='self',
              show_default=True,
              help='allow a configurable --suppress-cc=self git option')
@click.pass_context
def pr(ctx, fork, upstream, to, conf, skip_fmt, suppress_cc):
    """Mildly opinionated patch submission utility.

    This command prepares a feature branch as a patch series and sends the
    patches to the v-dev mailing list for review. It takes care of some of the
    detailed work like ensuring that a remote branch is list the cover letter.

    The tool is opinionated but configurable. Here is an example workflow:

       \b
       git checkout -b my-feature-x upstream/master
       << hack, hack, hack >>
       vtools pr

    The result of that example would be a patch series containing the feature-x
    changes being sent to the v-dev mailing list referencing the remote branch
    `origin/my-feature-x-v1`. The `-v1` suffix is automatically added. Branches
    for revisions later than version 1 are expected to be named with the target
    version. A workflow for creating the next version might look like:

       \b
       git checkout -b my-feature-x-v2 upstream/master
       git reset --hard my-feature-x
       << hack, hack, hack >>
       vtools pr

    The `upstream` branch refers to the branch against which the local branch
    should be compared when generating patches. This is generally
    github.com:vectorizedio/v.git@master, and (confusingly) it is often a branch
    named `master` in the `upstream` remote. This tool assumes that this is the
    upstream tracking branch for the local branch, but may be overridden with
    the `-u/--upstream` option. This is likely to be a personal preference, and
    as such should be supported as an override in the `vtools.yml`. The
    following Asana ticket tracks this feature
    https://app.asana.com/0/1149841353291489/1156266039071654.

    The `-f/--fork` option specifies the remote to which the feature branches
    will be pushed. It is common for this to be `origin` (default), or
    `your-name`.
    """
    repo = g.Repo(os.getcwd(), search_parent_directories=True)
    local_branch = repo.active_branch

    if "/" in local_branch.name:
        # TODO: something to investigate
        logging.info("WARNING: forward-slash in branch name. YMMV")

    remote = repo.remote(fork)
    logging.info("Updating remote references")
    remote.fetch()

    # head against which patches will be created. if not specified it will
    # default to the local branch's upstream tracking branch. this is typically
    # configured when creating a feature branch:
    #
    #    git checkout -b feature-x upstream/master
    #
    # normally this will be the branch https://github.com/vectorizedio/v @
    # master. however it is configurable here to allow flexibility: it is
    # possible that we grow large enough to warrant a hierarchical model in
    # which patch sets pass through maintainer trees first, or pass through a
    # ci/cd pipeline before merging to master.
    if not upstream:
        upstream = local_branch.tracking_branch()
        if not upstream:
            ctx.fail("Please configure an upstream branch. See --help output.")
        logging.info(f"fetching from {upstream.remote_name}")
        repo.remote(upstream.remote_name).fetch()
        upstream = upstream.name

    logging.info("Using upstream branch: {}".format(upstream))

    if not skip_fmt:
        logging.info("Checking proper formatting of source code.")
        fmt.all_changed(conf, check=True, ref=upstream)

    # create name for remote branch. the expected name is {local_branch}-vV. a
    # local branch without a -vV suffix is an alias for a suffix of -v1.
    remote_branch_prev = None
    m = re.match("^.+-v(?P<version>\d+)$", local_branch.name)
    if not m:
        version = 1
        remote_branch = "{}-v{}".format(local_branch.name, version)
        remote_branch_no_version = local_branch.name
    else:
        version = int(m.group("version"))
        remote_branch = local_branch
        remote_branch_no_version = re.sub("-v\d+$", "", local_branch.name)
        if version > 1:
            remote_branch_prev = "{}-v{}".format(remote_branch_no_version,
                                                 version - 1)

    # push up a remote branch with the changes that will be added to the cover
    # letter. if a remote branch has the same name, ask about force pushing.
    force = False
    remote_ref = next((ref for ref in remote.refs
                       if ref.name == "{}/{}".format(fork, remote_branch)),
                      None)
    if remote_ref and remote_ref.commit != local_branch.commit:
        force = click.confirm("Branch \"{}\" already exists. Force push? "
                              "".format(remote_branch),
                              default=True)

    if not remote_ref or force:
        refspec = "{}:{}".format(local_branch.name, remote_branch)
        logging.info("Pushing remote({}) branch: {}".format(fork, refspec))
        remote.push(refspec, force=force)

    # build the patch series in a staging directory
    staging_dir = tempfile.mkdtemp()
    format_args = [
        "git", "format-patch", "-v{}".format(version), "--cover-letter", "-o",
        staging_dir, upstream
    ]
    logging.info("Generating patches: {}".format(" ".join(map(
        str, format_args))))
    subprocess.run(format_args)

    if len(os.listdir(staging_dir)) == 0:
        ctx.fail("No patches generated. Check configured upstream branch.")

    if remote_branch_prev:
        cover_letter_msg = "<< insert commentary (no prev notes found) >>"
        remote_ref_prev = next(
            (ref for ref in remote.refs
             if ref.name == "{}/{}".format(fork, remote_branch_prev)), None)
        if remote_ref_prev:
            try:
                cover_letter_msg = repo.git.notes("show",
                                                  remote_ref_prev.commit)
                cover_letter_msg += """

Changed since v{}:
  - what
  - has
  - changed""".format(version - 1)
            except:
                pass
    else:
        cover_letter_msg = "<< insert commentary >>"

    # Customize the cover letter. First we replace the subject with a machine
    # generated value that should remain constant across patch series versions,
    # and then we add a reference to the remote branch.
    cover_letter_path = os.path.join(
        staging_dir, "v{}-0000-cover-letter.patch".format(version))

    with open(cover_letter_path, "r") as f:
        cover_letter = f.read()

    cover_letter = re.sub("\*\*\* SUBJECT HERE \*\*\*",
                          remote_branch_no_version, cover_letter)

    cover_letter_template = """REPLACE THIS WITH MESSAGE

The following patches are available at:

    x-patchouli: {url} {head}

Generated with `vtools pr` @ {sha1!s:7.7}"""

    cover_letter = re.sub(
        "\*\*\* BLURB HERE \*\*\*",
        cover_letter_template.format(url=next(repo.remote(fork).urls),
                                     head=remote_branch,
                                     sha1=local_branch.commit), cover_letter)

    with open(cover_letter_path, "w") as f:
        f.write(cover_letter)

    # for creating diff
    orig_cover_letter_path = "{}.orig".format(cover_letter_path)
    shutil.copyfile(cover_letter_path, orig_cover_letter_path)

    with open(cover_letter_path, "r") as f:
        cover_letter = f.read()
        cover_letter = re.sub("REPLACE THIS WITH MESSAGE", cover_letter_msg,
                              cover_letter)
    with open(cover_letter_path, "w") as f:
        f.write(cover_letter)

    click.edit(filename=cover_letter_path)

    # compute diff
    diff = subprocess.run(
        ("diff", "-u", orig_cover_letter_path, cover_letter_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False)
    grep = subprocess.run(("grep", "^\+"),
                          input=diff.stdout,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          check=False)
    assert grep.returncode in (0, 1)
    sed = subprocess.check_output(("sed", "-E", "/^\+\+\+/d"),
                                  input=grep.stdout)
    note = subprocess.check_output(("sed", "-E", "s/^\+//"), input=sed)
    os.remove(orig_cover_letter_path)

    note_path = os.path.join(staging_dir, "note")
    with open(note_path, "w") as f:
        f.write(note.decode())

    do_send = click.confirm("Send patch set to {}?".format(to), default=True)
    if do_send:
        # add notes to commit
        repo.git.notes("add", "-f", "-F", note_path, local_branch.commit)
        os.remove(note_path)

        send_mail_args = [
            "git", "send-email", "--confirm=never", "--to", to, staging_dir
        ]
        if len(suppress_cc) > 0:
            send_mail_args.append(f"--suppress-cc={suppress_cc}")
        subprocess.run(send_mail_args)
        logging.info("Sending patches: {}".format(" ".join(send_mail_args)))

    logging.info("Cleaning up staging dir: {}".format(staging_dir))
    shutil.rmtree(staging_dir)
