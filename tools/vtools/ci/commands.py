import yaml

import click
import git

from absl import logging
from google.cloud.devtools import cloudbuild_v1

from ..vlib import config


@click.group(short_help='Interact with the CI system.')
def ci():
    pass


@ci.command(short_help='trigger execution of CI job for current commit.')
@click.option('--build-type',
              help=('Configure the pipeline so that the given build type is '
                    'triggered.'),
              type=click.Choice(['debug', 'release', 'dev'],
                                case_sensitive=False),
              default=None)
@click.option('--clang',
              help='Set clang as the compiler to use in the CI pipeline.',
              is_flag=True)
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def trigger(build_type, clang, conf):
    """Triggers a CI job for the current commit.

    Requires a Github API token and credentials for Google Cloud Platform. The
    former in an access-token config entry of a 'github' section in the
    $HOME/.gitconfig file:

      [github]
        access-token = <your-token>

    For the GCP platform, it is expected that `gcloud auth` has been executed
    and configured so that access to the redpandaci project of the vectorizedio
    GCP account is allowed. If any of these is not defined, the command will
    fail.

    Note that this command ignores uncommitted changes, and only works on
    commits that have been pushed to any of the vectorizedio/v forks. There is
    no need to specify the fork since github allows to checkout any commit made
    to a fork via the main upstream repo (vectorizedio/v). If the current
    commit on which this command is being executed on (git rev-parse HEAD) is
    not available upstream in any of the forks, the execution will fail.
    """
    # TODO: add --push (and --force) flags to push before triggering. We need
    # to determine first which remote (origin, upstream, etc.) to push to.

    vconfig = config.VConfig(config_file=conf,
                             build_type=build_type,
                             clang=clang)

    with open(f'{vconfig.src_dir}/tools/ci/gcbuild-ci.yml') as f:
        gcb_conf = yaml.load(f, Loader=yaml.FullLoader)

    r = git.Repo(f'{vconfig.src_dir}')
    sha1 = r.head.object.hexsha

    git_conf = r.config_reader()
    gh_token = git_conf.get_value('github', 'access-token', None)
    if not gh_token:
        raise Exception("No 'access-token' in ~/.gitconfig 'github' section")

    client = cloudbuild_v1.CloudBuildClient()

    gcb_conf['substitutions'] = {
        'COMMIT_SHA': sha1,
        '_COMPILER': 'clang' if vconfig.compiler == 'clang' else 'gcc',
        '_BUILD_TYPE': vconfig.build_type,
        '_GITHUB_API_TOKEN': gh_token,
    }

    # workaround for lack of automatic conversion in gcb python API
    timeout_secs = gcb_conf.get('timeout', 3600)
    timeout_secs = timeout_secs.replace('s', '')
    timeout_duration = cloudbuild_v1.types.Duration(seconds=int(timeout_secs))
    gcb_conf['timeout'] = timeout_duration

    # workaround for camelCase vs snake_case mismatch in gcb python API
    machine_type = gcb_conf.get('options', {}).get('machineType', None)
    if machine_type:
        gcb_conf['options']['machine_type'] = machine_type
        gcb_conf['options'].pop('machineType')

    logging.info(f'Triggering test COMMIT={sha1},COMPILER={vconfig.compiler},'
                 f'BUILD_TYPE={vconfig.build_type}')

    client.create_build('redpandaci', gcb_conf)
