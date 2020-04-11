import click
import docker
import glob
import json
import lddwrap
import os
import pathlib
import shutil
import time
import yaml

from absl import logging

from ..vlib import config
from ..vlib import terraform
from ..vlib import shell


@click.group(short_help='run unit and integration tests')
def test():
    pass


@test.command(short_help='rpk unit tests')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def go(conf):
    vconfig = config.VConfig(conf)
    with os.scandir(vconfig.go_src_dir) as it:
        for fd in it:
            if not fd.name.startswith('.') and fd.is_dir():
                shell.run_subprocess(
                    f'cd {vconfig.go_src_dir}/{fd.name}/pkg && go test ./...',
                    env=vconfig.environ)


@test.command(short_help='python tests')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def py(conf):
    vconfig = config.VConfig(conf)
    shell.run_subprocess(
        (f'python -m unittest discover -s {vconfig.src_dir}/tools/vtools'
         ' -v -p "*_test.py"'),
        env=vconfig.environ)


@test.command(short_help='redpanda unit tests')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', 'dev'],
                                case_sensitive=False),
              default=None)
@click.option('--clang',
              help=('Test binaries compiled by clang.'),
              is_flag=True)
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--args', help=('passes raw args to testing'), default=None)
def cpp(build_type, conf, clang, args):
    vconfig = config.VConfig(config_file=conf,
                             build_type=build_type,
                             clang=clang)

    if vconfig.compiler == 'clang':
        # define LD_LIBRARY_PATH for clang builds
        ld_path = (f'/lib:/lib64:/usr/local/lib:/usr/local/lib64:'
                   f'{vconfig.external_path}/lib:'
                   f'{vconfig.external_path}/lib64')
        logging.info(f'Setting LD_LIBRARY_PATH={ld_path}')
        vconfig.environ['LD_LIBRARY_PATH'] = ld_path

    args = f' {args}' if args else '-R \".*_rp(unit|bench|int)$\"'
    shell.run_subprocess(f'cd {vconfig.build_dir} && '
                         f'ctest '
                         f' {"-V" if os.environ.get("CI") else ""} ' + args,
                         env=vconfig.environ)


@test.command(short_help='redpanda integration (ducktape) tests')
@click.argument('tests', default='tests/rp/ducktape/tests')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', 'dev'],
                                case_sensitive=False),
              default=None)
@click.option('-n', '--nodes', help='Num of nodes in the cluster', default=7)
@click.option('--clang', help='Test binaries compiled by clang.', is_flag=True)
@click.option('--skip-build-img', help='Do not rebuild images.', is_flag=True)
@click.option('--use-existing-cluster',
              help='Use existing cluster that has been deployed with vtools.',
              is_flag=True)
@click.option('--rp-extra-conf',
              help=('Path to configuration options to use to extend or '
                    'override the default options used in tests.'),
              default=None)
@click.option('--conf',
              help=('Path to vtools configuration file. If not given, a '
                    '.vtools.yml file is searched recursively starting from '
                    'the current working directory'),
              default=None)
def pz(build_type, conf, clang, skip_build_img, tests, nodes,
       use_existing_cluster, rp_extra_conf):
    vconfig = config.VConfig(config_file=conf,
                             build_type=build_type,
                             clang=clang)
    d = docker.from_env()
    tests_dir = f'{vconfig.src_dir}/tests'
    docker_dir = f'{tests_dir}/docker'

    # ensure output temp dir exists
    os.makedirs(f'{vconfig.build_root}/tests/', exist_ok=True)

    if not skip_build_img:
        # when the ducktape container has been previously executed, some .pyc
        # files are left that are owned by root, so we change their ownership
        shell.run_oneline(f'sudo chown $USER:$USER -R {vconfig.src_dir}/tests')

        # docker-py does not support custom context dir AND custom path for
        # Dockerfile, so we need to shell out
        cmd = (f'docker build -t ducktape '
               f'-f {docker_dir}/Dockerfile.ducktape {tests_dir}')
        shell.run_oneline(cmd, env=vconfig.environ)

        if not use_existing_cluster:
            # TODO: ensure only one tarball exists
            # TODO: build if it doesn't exist
            for f in glob.glob(f'{vconfig.build_dir}/dist/tar/*.tar.gz'):
                logging.debug(f'Copying {vconfig.build_dir}/dist/tar/*.tar.gz')
                shutil.copy(f, f'{docker_dir}/redpanda.tar.gz')

            # only build panda-node if we're running cluster locally
            cmd = (f'docker build -t panda-node '
                   f'-f {docker_dir}/Dockerfile.panda-node {docker_dir}')
            shell.run_oneline(cmd, env=vconfig.environ)

            os.remove(f'{docker_dir}/redpanda.tar.gz')

    if use_existing_cluster:
        # we assume a cluster has already been deployed with 'vtools deploy'
        _generate_ducktape_cluster_config(
            vconfig,
            filename=f'{vconfig.build_root}/tests/cluster.json',
            use_existing=True)
    else:
        # deployed a local cluster using docker-compose
        _generate_docker_compose_config(
            num_nodes=nodes,
            filename=f'{vconfig.build_root}/tests/docker-compose.yml')
        _generate_ducktape_cluster_config(
            vconfig,
            filename=f'{vconfig.build_root}/tests/cluster.json',
            num_nodes=nodes)
        _create_network(d, 'rp')

        # bring cluster up (compose doesn't have a python API, so we shell)
        cmd = (f'cd {vconfig.build_root}/tests && '
               f'docker-compose -p rp up --force-recreate -t 1 -d')
        shell.run_oneline(cmd, env=vconfig.environ)

        # wait for cluster to be up
        max_retries = 10
        curr = 1
        filters = {'status': 'running', 'ancestor': 'panda-node'}
        while len(d.containers.list(filters=filters)) != nodes:
            if curr == max_retries:
                raise Exception("Timed out waiting for cluster to be up")
            time.sleep(2)
            curr += 1

    # run ducktape tests
    dt_ret = _run_ducktape_tests(vconfig, d, tests, use_existing_cluster,
                                 rp_extra_conf)

    if not use_existing_cluster:
        # teardown compose cluster
        cmd = (f'cd {vconfig.build_root}/tests/ && '
               'docker-compose -p rp down -t 1')
        # shell.run_oneline(cmd, env=vconfig.environ)

    if dt_ret != 0:
        logging.fatal(f'ducktape returned non-zero code {dt_ret}')


def _run_ducktape_tests(vconfig,
                        docker_client,
                        tests,
                        use_existing=False,
                        rp_extra_conf=None):
    volumes = {
        f'{vconfig.build_root}/tests/ducktape-config': {
            'bind': '/root/.ducktape',
            'mode': 'rw'
        },
        f'{vconfig.src_dir}/tests': {
            'bind': '/root/tests',
            'mode': 'rw'
        },
        f'{vconfig.build_root}/tests': {
            'bind': '/build/tests',
            'mode': 'rw'
        },
    }

    environment = {}
    if rp_extra_conf:
        rp_extra_conf = os.path.abspath(rp_extra_conf)
        volumes.update({
            rp_extra_conf: {
                'bind': '/root/rp-extra-config.yml',
                'mode': 'ro'
            }
        })
        environment.update({'RP_EXTRA_CONF': '/root/rp-extra-config.yml'})

    # NOTE: these paths are within the container (see volumes above)
    dt_flags = [
        '--results-root=/build/tests/results',
        '--cluster-file=/build/tests/cluster.json',
        '--cluster=ducktape.cluster.json.JsonCluster', '--exit-first',
        '--debug', tests
    ]

    if use_existing:
        network = 'host'
    else:
        network = 'rp'

    container = docker_client.containers.run('ducktape',
                                             command=dt_flags,
                                             name='dt',
                                             network=network,
                                             volumes=volumes,
                                             environment=environment,
                                             working_dir='/root',
                                             detach=True)

    for l in container.logs(stream=True, follow=True):
        logging.debug(l)

    ecode = container.wait()['StatusCode']

    container.remove()

    return ecode


def _generate_docker_compose_config(num_nodes=0, filename=None):
    nodes = {}

    for i in range(1, num_nodes + 1):
        node = {
            'image': 'panda-node',
            'volumes': [f'./data/n{i}:/var/lib/redpanda'],
            'cpus': 2,
            'privileged': True,
        }
        if i > 1:
            # so that n1 gets first IP in range, n2 second, etc..
            node.update({'depends_on': [f'n{i-1}']})
        nodes.update({f'n{i}': node})

    with open(filename, 'w') as f:
        yaml.dump(
            {
                'version': '2.4',
                'services': nodes,
                'networks': {
                    'default': {
                        'external': {
                            'name': 'rp'
                        }
                    }
                }
            }, f)


def _create_network(docker_client, name):
    if docker_client.networks.list(names=[name]):
        # no need to recreate it
        return

    ipam_pool = docker.types.IPAMPool(subnet='192.168.52.0/24',
                                      gateway='192.168.52.254')
    ipam_config = docker.types.IPAMConfig(pool_configs=[ipam_pool])
    docker_client.networks.create(name, driver="bridge", ipam=ipam_config)


def _generate_ducktape_cluster_config(vconfig,
                                      use_existing=False,
                                      num_nodes=3,
                                      filename=None):
    nodes = []

    if use_existing:
        # generate configuration from terraform output data
        tf_out = terraform._get_tf_outputs(vconfig, 'cluster')

        os.makedirs(f'{vconfig.ansible_tmp_dir}', exist_ok=True)
        invfile = f'{vconfig.ansible_tmp_dir}/hosts.ini'
        with open(invfile, 'w') as f:
            zipped = zip(tf_out['ip']['value'], tf_out['private_ips']['value'])
            for ip, pip in zipped:
                nodes.append({
                    'externally_routable_ip': f'{ip}',
                    'ssh_config': {
                        'host': f'{ip}',
                        'hostname': f'{pip}',
                        'identityfile': f'/root/.ssh/id_rsa',
                        'password': '',
                        'port': 22,
                        'user': 'root'
                    }
                })
    else:
        # generate configuration for the docker-compose cluster
        for i in range(1, num_nodes + 1):
            nodes.append({
                'externally_routable_ip': f'192.168.52.{i+1}',
                'ssh_config': {
                    'host': f'n{i}',
                    'hostname': f'n{i}',
                    'identityfile': f'/root/.ssh/id_rsa',
                    'password': '',
                    'port': 22,
                    'user': 'root'
                }
            })

    with open(filename, 'w') as f:
        json.dump({'nodes': nodes}, f)


@test.command(short_help='print runtime dependencies of a binary')
@click.option("--binary", required=True, help="path to binary")
def print_deps(binary):
    """prints ldd output"""
    binpath = pathlib.Path(binary)
    deps = lddwrap.list_dependencies(path=binpath)
    for dep in deps:
        click.echo("Found dep: %s" % dep)


@test.command(short_help='runs npm test on build/node')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def js(conf):
    vconfig = config.VConfig(conf)

    shell.run_subprocess(f'cd {vconfig.node_build_dir} && '
                         f'npm test',
                         env=vconfig.environ)
