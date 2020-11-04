import hashlib
import os
import subprocess

from absl import logging

from . import shell


def _run_in_docker(command, extra_args=''):
    if not os.path.exists('.git/'):
        logging.fatal(
            "Unable to find .git/ folder. This command needs to be executed "
            "from the project's root folder.")
    shell.run_oneline(
        f'docker run --rm -ti --entrypoint=/workspace/build/venv/v/bin/vtools'
        f'  -w /workspace -v $PWD:/workspace -v $PWD/dbuild:/workspace/build'
        f'  gcr.io/redpandaci/builder-clang-release:latest'
        f'    build {command} --conf tools/ci/vtools-clang-release.yml'
        f'      --skip-external {extra_args}')


def _get_toolchain_image_metadata(vconfig):
    """Given a instance of VConfig, return image metadata that describes which
    images the toolchain needs, where are their Dockerfiles, what are their
    dependency's SHA, and which arguments are to be passed when building them
    (``--build-arg`` flag to ``docker build``).
    """
    toolchain_images = {
        'base': {
            'name': 'gcr.io/redpandaci/base',
            'files': [f'{vconfig.src_dir}/tools/install-deps.sh'],
            'dockerfile': f'{vconfig.src_dir}/tools/ci/docker/Dockerfile.base',
            'build_args': [],
            'needs': [],
        },
        'clang': {
            'name':
            'gcr.io/redpandaci/clang',
            'files': [
                f'{vconfig.src_dir}/tools/vtools/vlib/clang.py',
                f'{vconfig.src_dir}/tools/ci/vtools-clang-release.yml',
                f'{vconfig.src_dir}/cmake/caches/llvm.cmake',
            ],
            'dockerfile':
            f'{vconfig.src_dir}/tools/ci/docker/Dockerfile.clang',
            'needs': ['base'],
        },
        'golang': {
            'name':
            'gcr.io/redpandaci/golang',
            'files': [
                f'{vconfig.src_dir}/src/go/rpk/go.mod',
                f'{vconfig.src_dir}/src/go/rpk/go.sum',
                f'{vconfig.src_dir}/src/go/metrics/go.mod',
                f'{vconfig.src_dir}/src/go/metrics/go.sum',
                f'{vconfig.src_dir}/tools/ci/vtools-gcc-release.yml',
            ],
            'dockerfile':
            f'{vconfig.src_dir}/tools/ci/docker/Dockerfile.golang',
            'needs': ['base'],
        },
        'node': {
            'name':
            'gcr.io/redpandaci/node',
            'files': [
                f'{vconfig.node_src_dir}/package.json',
                f'{vconfig.node_src_dir}/package-lock.json',
                f'{vconfig.node_src_dir}/generate-entries.sh',
                f'{vconfig.src_dir}/tools/ci/docker/Dockerfile.node',
                f'{vconfig.src_dir}/tools/ci/vtools-gcc-release.yml',
            ],
            'dockerfile':
            f'{vconfig.src_dir}/tools/ci/docker/Dockerfile.node',
            'needs': ['base'],
        },
        'builder': {
            'name':
            'gcr.io/redpandaci/builder',
            'files': [
                f'{vconfig.src_dir}/tools/vtools/vlib/cmake.py',
                f'{vconfig.src_dir}/3rdparty.cmake.in',
                f'{vconfig.src_dir}/CMakeLists.txt',
                f'{vconfig.src_dir}/tools/ci/vtools-{vconfig.compiler}-{vconfig.build_type}.yml',
                f'{vconfig.src_dir}/tools/ci/docker/Dockerfile.builder',
                f'{vconfig.src_dir}/tools/vtools/vlib/cmake.py',
            ],
            'dockerfile':
            f'{vconfig.src_dir}/tools/ci/docker/Dockerfile.builder',
            'needs': ['clang', 'golang', 'node'],
        },
    }

    # add shas
    toolchain_images['base'].update(
        {'sha': _get_sha('base', toolchain_images)})
    toolchain_images['clang'].update(
        {'sha': _get_sha('clang', toolchain_images)})
    toolchain_images['golang'].update(
        {'sha': _get_sha('golang', toolchain_images)})
    toolchain_images['node'].update(
        {'sha': _get_sha('node', toolchain_images)})
    toolchain_images['builder'].update(
        {'sha': _get_sha('builder', toolchain_images)})

    # add name, tag and args
    toolchain_images['base'].update({
        'name_tag':
        f'{toolchain_images["base"]["name"]}:{toolchain_images["base"]["sha"]}',
        'build_args': [],
    })
    toolchain_images['clang'].update({
        'name_tag':
        f'{toolchain_images["clang"]["name"]}:{toolchain_images["clang"]["sha"]}',
        'build_args': [
            f'--build-arg BASE_SHA={toolchain_images["base"]["sha"]}',
        ],
    })
    toolchain_images['golang'].update({
        'name_tag':
        f'{toolchain_images["golang"]["name"]}:{toolchain_images["golang"]["sha"]}',
        'build_args': [
            f'--build-arg BASE_SHA={toolchain_images["base"]["sha"]}',
        ],
    })
    toolchain_images['node'].update({
        'name_tag':
        f'{toolchain_images["node"]["name"]}:{toolchain_images["node"]["sha"]}',
        'build_args': [
            f'--build-arg BASE_SHA={toolchain_images["base"]["sha"]}',
        ],
    })
    toolchain_images['builder'].update({
        'name_tag':
        f'{toolchain_images["builder"]["name"]}:{vconfig.compiler}-{vconfig.build_type}-{toolchain_images["builder"]["sha"]}',
        'build_args': ([
            f'--build-arg CLANG_SHA={toolchain_images["clang"]["sha"]}',
            f'--build-arg GOLANG_SHA={toolchain_images["golang"]["sha"]}',
            f'--build-arg NODE_SHA={toolchain_images["node"]["sha"]}',
            f'--build-arg COMPILER={vconfig.compiler}',
            f'--build-arg BUILD_TYPE={vconfig.build_type}',
        ]),
    })

    return toolchain_images


def _get_image(img, toolchain_images, vconfig):
    exists = _pull_image(img, toolchain_images)
    if not exists:
        logging.debug('Image or tag not found, building and pushing it')
        _build_image(img, toolchain_images, vconfig)


def _pull_image(img, toolchain_images):
    """Attempts to pull an image and returns True if it successfully pulled it.
    If image is not available on the referenced registry False is returned. It
    raises an error if there's another error.
    """
    logging.debug(f'Pulling image {toolchain_images[img]["name_tag"]}')
    p = subprocess.run(f'docker pull {toolchain_images[img]["name_tag"]}',
                       capture_output=True,
                       text=True,
                       shell=True)
    if p.returncode != 0:
        if 'not found' in p.stderr:
            return False
        else:
            logging.fatal('docker pull command failed: {p.stderr}')
    return True


def _build_image(img, toolchain_images, vconfig):
    """Builds given image and it's dependencies. ``img`` is expected to be a member
    of the ``toolchain_images`` global dictionary defined in this module.
    """
    for d in toolchain_images[img]['needs']:
        _get_image(d, toolchain_images, vconfig)
    _docker_build(img, toolchain_images, vconfig)


def _docker_build(img, toolchain_images, vconfig):
    """Build and push given image"""
    subprocess.run(
        f'docker build'
        f'  {" ".join(toolchain_images[img]["build_args"])}'
        f'  -t {toolchain_images[img]["name_tag"]}'
        f'  -f {toolchain_images[img]["dockerfile"]}'
        f'  {vconfig.src_dir}',
        shell=True,
        check=True,
    )
    subprocess.run(f'docker push {toolchain_images[img]["name_tag"]}',
                   shell=True,
                   check=True)


def _get_sha(img, toolchain_images):
    h = hashlib.sha256()
    buf = []

    # fill buffer with file dependencies
    for fname in toolchain_images[img]['files']:
        with open(fname, 'r') as f:
            buf.append(f.read())

    # append sha of dependencies
    for i in toolchain_images[img]['needs']:
        buf.append(_get_sha(i, toolchain_images))

    buf = ''.join(buf).encode('utf-8')

    h.update(buf)

    # return first 10 characters
    return h.hexdigest()[:10]
