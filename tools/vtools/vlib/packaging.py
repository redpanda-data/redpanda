import git
import glob
import io
import os
import pathlib
import lddwrap
import shutil
import subprocess
import tarfile

from absl import logging
from . import fs
from . import shell
from . import templates

VENDOR = "Vectorized Inc."
REDPANDA_NAME = "redpanda"
REDPANDA_DESCRIPTION = 'Redpanda, the fastest queue in the West'
REDPANDA_CATEGORY = "Applications/Misc"
REDPANDA_URL = "https://vectorized.io/product"
LICENSE = 'Proprietary and confidential'
CODENAME = "pandacub"
VERSION = "0.0"
REVISION = "0000000"
RELEASE = "1"


def _get_dependencies(binary, vconfig):
    logging.debug(f"Getting dependencies of {binary}")

    if vconfig.compiler == 'clang':
        libasan_path = shell.run_oneline(
            f'{vconfig.clang_path}/bin/clang -print-file-name=libclang_rt.asan-x86_64.so',
            env=vconfig.environ)
    else:
        libasan_path = shell.run_oneline('gcc -print-file-name=libasan.so',
                                         env=vconfig.environ)
    compiler_libs_path = os.path.dirname(libasan_path)

    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = (f'{vconfig.external_path}/lib:'
                              f'{vconfig.external_path}/lib64:'
                              f'{compiler_libs_path}')
    deps = lddwrap.list_dependencies(path=pathlib.Path(binary), env=env)

    libs = {}
    for dep in deps:
        if not dep.found:
            logging.fatal(f"Cannot find location for {dep.soname}")

        if not dep.soname:
            # linux libs have soname=None
            if 'ld-' in str(dep.path):
                # we ignore all but the loader
                libs['ld.so'] = os.path.realpath(dep.path)
            continue

        libs[dep.soname] = os.path.realpath(dep.path)

    return libs


def _relocable_tar_package(dest, execs, configs, admin_api_swag, vconfig):
    logging.info(f"Creating relocable tar package {dest}")
    gzip_process = subprocess.Popen(f"pigz -f > {dest}",
                                    shell=True,
                                    env=vconfig.environ,
                                    stdin=subprocess.PIPE)
    thunk = b'''\
#!/usr/bin/env bash

command="$(readlink -f "$0")"
basename="$(basename "$command")"
directory="$(dirname "$command")/.."
ldso="$directory/libexec/$basename"
realexe="$directory/libexec/$basename.bin"
binpath="$directory/bin"
export LD_LIBRARY_PATH="$directory/lib"
export PATH="${binpath}:${PATH}"
exec -a "$0" "$ldso" "$realexe" "$@"
'''

    ar = tarfile.open(fileobj=gzip_process.stdin, mode='w|')
    all_libs = {}
    for exe in execs:
        logging.debug(f"Adding '{exe}' executable to relocable tar")
        basename = os.path.basename(exe)
        ar.add(exe, arcname='libexec/' + basename + '.bin')
        ti = tarfile.TarInfo(name='bin/' + basename)
        ti.size = len(thunk)
        ti.mode = 0o755
        ti.mtime = os.stat(exe).st_mtime
        ar.addfile(ti, fileobj=io.BytesIO(thunk))
        ti = tarfile.TarInfo(name='libexec/' + basename)
        ti.type = tarfile.SYMTYPE
        ti.linkname = '../lib/ld.so'
        ti.mtime = os.stat(exe).st_mtime
        ar.addfile(ti)
        all_libs.update(_get_dependencies(exe, vconfig))
    for lib, location in all_libs.items():
        logging.debug(f"Adding '{location}' lib to relocable tar")
        ar.add(location, arcname="lib/" + lib)
    for conf in configs:
        ar.add(conf, arcname=f"conf/{os.path.basename(conf)}")
    for swag in admin_api_swag:
        arcname = f"etc/redpanda/admin-api-doc/{os.path.basename(swag)}"
        ar.add(swag, arcname=arcname)
    ar.close()
    gzip_process.communicate()


def red_panda_tar(input_tar, dest_path):
    logging.info("Creating tarball package")
    tar_dir = os.path.join(dest_path, "tar")
    os.makedirs(tar_dir, exist_ok=True)
    tar_name = f'redpanda-{VERSION}-{RELEASE}_{REVISION}.tar.gz'
    tar_file = os.path.join(tar_dir, tar_name)
    shutil.copy(input_tar, tar_file)


def _rpm_tree(dest):
    for dir in ['BUILD', 'BUILDROOT', 'RPMS', 'SOURCES', 'SPECS', 'SRPMS']:
        os.makedirs(f"{dest}/{dir}", exist_ok=True)


def _pkg_context():
    return {
        "name": REDPANDA_NAME,
        "version": VERSION,
        "summary": REDPANDA_DESCRIPTION,
        "desc": REDPANDA_DESCRIPTION,
        "release": RELEASE,
        "license": LICENSE,
        "revision": REVISION,
        "codename": CODENAME
    }


def red_panda_rpm(input_tar, dest_path, src_dir, env):
    logging.info("Creating RPM package")
    # prepare RPM sources
    rpm_tree_root = os.path.join(dest_path, "rpm")
    shutil.rmtree(rpm_tree_root, ignore_errors=True)
    _rpm_tree(os.path.join(dest_path, "rpm"))
    fs.force_link(input_tar, os.path.join(dest_path,
                                          "rpm/SOURCES/redpanda.tar"))
    shutil.copytree(f'{src_dir}/packaging/common',
                    os.path.join(dest_path, "rpm/common"),
                    ignore=_is_template)
    # render templates
    package_ctx = _pkg_context()
    package_ctx['source_tar'] = "redpanda.tar"
    spec_template = f"{src_dir}/packaging/rpm/redpanda.spec.j2"
    spec = os.path.join(dest_path, "rpm/SPECS/redpanda.spec")
    templates.render_to_file(spec_template, spec, package_ctx)
    _render_systemd_templates(os.path.join(dest_path, "rpm/common"),
                              {'redhat': True}, src_dir)
    # build RPM
    nproc = os.cpu_count()
    shell.run_subprocess(
        f'rpmbuild -bb --define \"_topdir {rpm_tree_root}\" --define \"_binary_payload w2T{nproc}.xzdio\" {spec}',
        env=env)


def red_panda_deb(input_tar, dest_path, src_dir, env):
    logging.info("Creating DEB package")
    debian_dir = os.path.join(dest_path, "debian/redpanda")
    os.makedirs(debian_dir, exist_ok=True)
    shutil.rmtree(debian_dir)
    shutil.copytree(f"{src_dir}/packaging/debian/debian",
                    os.path.join(debian_dir, 'debian'))
    target_tar_name = f"debian/redpanda_{VERSION}-{RELEASE}.orig.tar.gz"
    fs.force_link(input_tar, os.path.join(dest_path, target_tar_name))
    common_path = os.path.join(dest_path, "debian/redpanda/common")
    shutil.copytree(f'{src_dir}/packaging/common',
                    common_path,
                    ignore=_is_template)

    # render templates
    package_ctx = _pkg_context()
    chglog_tmpl = f"{src_dir}/packaging/debian/changelog.j2"
    control_tmpl = f"{src_dir}/packaging/debian/control.j2"
    _render_systemd_templates(common_path, {"debian": True}, src_dir)
    for f in glob.glob(os.path.join(common_path, "systemd", "*")):
        shutil.copy(f, os.path.join(dest_path, "debian/redpanda/debian"))
    templates.render_to_file(
        chglog_tmpl, os.path.join(dest_path,
                                  "debian/redpanda/debian/changelog"),
        package_ctx)
    templates.render_to_file(
        control_tmpl,
        os.path.join(dest_path, "debian/redpanda/debian/control"), package_ctx)
    # build DEB
    shell.raw_check_output(f"tar -C {debian_dir} -xpf {input_tar}", env=env)
    shell.run_subprocess(f'cd {debian_dir} && debuild -rfakeroot -us -uc -b',
                         env=env)


def _is_template(source, files):
    return filter(lambda f: f.endswith(".j2"), files)


def _render_systemd_templates(dest_path, ctx, src_dir):
    root_dir = 'packaging/common/systemd/'
    jinja_ext = '.j2'
    files = ['redpanda.slice', 'redpanda.service', 'redpanda-tuner.service']
    for f in files:
        tmpl = f'{src_dir}/{root_dir}{f}{jinja_ext}'
        templates.render_to_file(tmpl, os.path.join(dest_path, 'systemd', f),
                                 ctx)
    shutil.copy(os.path.join(root_dir, "50-redpanda.preset"),
                os.path.join(dest_path, "systemd"))
    shutil.copy(os.path.join(root_dir, "redpanda-status.service"),
                os.path.join(dest_path, "systemd"))
    shutil.copy(os.path.join(root_dir, "redpanda-status.timer"),
                os.path.join(dest_path, "systemd"))


def create_packages(vconfig, formats, build_type):
    global VERSION
    global REVISION
    global RELEASE

    REVISION = os.environ.get('SHORT_SHA', None)
    if not REVISION:
        repo = git.Repo(os.getcwd(), search_parent_directories=True)
        if repo:
            REVISION = repo.git.rev_parse(repo.head.object.hexsha, short=7)
        else:
            REVISION = '000000'

    tag = os.environ.get('TAG_NAME', None)
    if tag and 'release-' in tag:
        VERSION = tag.replace('release-', '')
    else:
        RELEASE = "dev"

    execs = [
        f'{vconfig.build_dir}/bin/redpanda',
        f'{vconfig.go_out_dir}/rpk',
        f'{vconfig.external_path}/bin/hwloc-calc',
        f'{vconfig.external_path}/bin/hwloc-distrib',
        f'{vconfig.external_path}/bin/iotune',
    ]

    dist_path = os.path.join(vconfig.build_dir, "dist")

    configs = [os.path.join(vconfig.src_dir, "conf/redpanda.yaml")]
    admin_api_swag = glob.glob(
        os.path.join(vconfig.src_dir, "src/v/redpanda/admin/api-doc/*.json"))
    os.makedirs(dist_path, exist_ok=True)

    tar_name = 'redpanda.tar.gz'
    tar_path = f"{dist_path}/{tar_name}"
    _relocable_tar_package(tar_path, execs, configs, admin_api_swag, vconfig)

    if 'tar' in formats:
        red_panda_tar(tar_path, dist_path)
    if 'deb' in formats:
        red_panda_deb(tar_path, dist_path, vconfig.src_dir, vconfig.environ)
    if 'rpm' in formats:
        red_panda_rpm(tar_path, dist_path, vconfig.src_dir, vconfig.environ)
    os.remove(tar_path)
