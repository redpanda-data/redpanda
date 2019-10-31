#!/usr/bin/env python3
import argparse
import sys
import os
import logging
import re
import tarfile
import subprocess
import io
import functools
import shutil
import glob

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

from constants import *
from pkg_config import *
import shell
import templates
import fs

thunk = b'''\
#!/bin/bash

command="$(readlink -f "$0")"
basename="$(basename "$command")"
directory="$(dirname "$command")/.."
ldso="$directory/libexec/$basename"
realexe="$directory/libexec/$basename.bin"
binpath="$directory/bin"
LD_LIBRARY_PATH="$directory/lib" PATH="${binpath}:${PATH}" exec -a "$0" "$ldso" "$realexe" "$@"
'''


def _get_dependencies(binary):
    logger.debug("Getting dependencies of {}".format(binary))
    pattern = r'(.*) => (.*) \(0x[0-9a-f]{16}\)'
    libs = {}
    raw_lines = shell.raw_check_output("ldd %s" % binary).splitlines()
    lines = map(lambda line: line.strip(), raw_lines)
    for ldd_line in lines:
        match = re.search(pattern, ldd_line)
        if match is not None:
            libs[match.group(1)] = os.path.realpath(match.group(2))
        elif 'ld-' in ldd_line:
            libs['ld.so'] = os.path.realpath(ldd_line.split(' ')[0])
    return libs


def relocable_tar_package(dest, execs, configs, admin_api_swag):
    logger.info("Creating relocable tar package %s", dest)
    gzip_process = subprocess.Popen("pigz -f > %s" % dest,
                                    shell=True,
                                    stdin=subprocess.PIPE)
    ar = tarfile.open(fileobj=gzip_process.stdin, mode='w|')
    all_libs = {}
    for exe in execs:
        logger.debug("Adding '%s' executable to relocable tar", exe)
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
        all_libs.update(_get_dependencies(exe))
    for lib, location in all_libs.items():
        logger.debug("Adding '%s' lib to relocable tar", location)
        ar.add(location, arcname="lib/" + lib)
    for conf in configs:
        ar.add(conf, arcname="conf/%s" % os.path.basename(conf))
    for swag in admin_api_swag:
        arcname = "etc/redpanda/admin-api-doc/%s" % os.path.basename(swag)
        ar.add(swag, arcname=arcname)
    ar.close()
    gzip_process.communicate()


def _in_root(path):
    return os.path.join(RP_ROOT, path)


def red_panda_tar(input_tar, dest_path):
    logger.info("Creating tarball package")
    tar_dir = os.path.join(dest_path, "tar")
    os.makedirs(tar_dir, exist_ok=True)
    tar_name = 'redpanda-%s-%s_%s.tar.gz' % (VERSION, RELEASE, REVISION)
    tar_file = os.path.join(tar_dir, tar_name)
    shutil.copy(input_tar, tar_file)


def _rpm_tree(dest):
    for dir in ['BUILD', 'BUILDROOT', 'RPMS', 'SOURCES', 'SPECS', 'SRPMS']:
        os.makedirs("%s/%s" % (dest, dir), exist_ok=True)


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


def red_panda_rpm(input_tar, dest_path):
    logger.info("Creating RPM package")
    # prepare RPM sources
    rpm_tree_root = os.path.join(dest_path, "rpm")
    shutil.rmtree(rpm_tree_root, ignore_errors=True)
    _rpm_tree(os.path.join(dest_path, "rpm"))
    fs.force_link(input_tar, os.path.join(dest_path,
                                          "rpm/SOURCES/redpanda.tar"))
    shutil.copytree(_in_root('packaging/common'),
                    os.path.join(dest_path, "rpm/common"),
                    ignore=_is_template)
    # render templates
    package_ctx = _pkg_context()
    package_ctx['source_tar'] = "redpanda.tar"
    spec_template = _in_root("packaging/rpm/redpanda.spec.j2")
    spec = os.path.join(dest_path, "rpm/SPECS/redpanda.spec")
    templates.render_to_file(spec_template, spec, package_ctx)
    _render_systemd_templates(os.path.join(dest_path, "rpm/common"),
                              {'redhat': True})
    shutil.copy(
        os.path.join("packaging", "common", "systemd", "50-redpanda.preset"),
        os.path.join(dest_path, "systemd"))
    # build RPM
    shell.run_subprocess('rpmbuild -bb --define \"_topdir %s\" %s' %
                         (rpm_tree_root, spec))


def red_panda_deb(input_tar, dest_path):
    logger.info("Creating DEB package")
    debian_dir = os.path.join(dest_path, "debian/redpanda")
    os.makedirs(debian_dir, exist_ok=True)
    shutil.rmtree(debian_dir)
    shutil.copytree(_in_root("packaging/debian/debian"),
                    os.path.join(debian_dir, 'debian'))
    target_tar_name = "debian/redpanda_%s-%s.orig.tar.gz" % (VERSION, RELEASE)
    fs.force_link(input_tar, os.path.join(dest_path, target_tar_name))
    common_path = os.path.join(dest_path, "debian/redpanda/common")
    shutil.copytree(_in_root('packaging/common'),
                    common_path,
                    ignore=_is_template)

    # render templates
    package_ctx = _pkg_context()
    chglog_tmpl = _in_root("packaging/debian/changelog.j2")
    control_tmpl = _in_root("packaging/debian/control.j2")
    _render_systemd_templates(common_path, {"debian": True})
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
    shell.raw_check_output("tar -C %s -xpf %s" % (debian_dir, input_tar))
    shell.run_subprocess('cd %s && debuild -rfakeroot -us -uc -b' % debian_dir)


def _is_template(source, files):
    return filter(lambda f: f.endswith(".j2"), files)


def _render_systemd_templates(dest_path, ctx):
    root_dir = 'packaging/common/systemd/'
    jinja_ext = '.j2'
    files = ['redpanda.slice', 'redpanda.service', 'redpanda-tuner.service']
    for f in files:
        tmpl = _in_root(root_dir + f + jinja_ext)
        templates.render_to_file(tmpl, os.path.join(dest_path, 'systemd', f),
                                 ctx)


def create_packages(formats,
                    src_dir=RP_ROOT,
                    build_dir=RP_BUILD_ROOT,
                    build_type="",
                    external=list()):
    execs = [
        "%s/%s/bin/redpanda" % (build_dir, build_type),
        "%s/go/bin/rpk" % build_dir,
    ]
    execs.extend(external)
    dist_path = os.path.join(build_dir, build_type, "dist")
    configs = [os.path.join(src_dir, "conf/redpanda.yaml")]
    admin_api_swag = glob.glob(
        os.path.join(src_dir, "src/v/redpanda/admin/api-doc/*.json"))
    os.makedirs(dist_path, exist_ok=True)
    tar_name = 'redpanda.tar.gz'
    tar_path = "%s/%s" % (dist_path, tar_name)
    relocable_tar_package(tar_path, execs, configs, admin_api_swag)
    if 'tar' in formats:
        red_panda_tar(tar_path, dist_path)
    if 'deb' in formats:
        red_panda_deb(tar_path, dist_path)
    if 'rpm' in formats:
        red_panda_rpm(tar_path, dist_path)
    os.remove(tar_path)


def main():
    parser = argparse.ArgumentParser(description='packaging helper')
    parser.add_argument(
        '--build-dir',
        type=str,
        default=RP_BUILD_ROOT,
        help='path to folder containing build artifacts')
    parser.add_argument(
        '--formats',
        choices=['tar', 'rpm', 'deb'],
        nargs='+',
        help='space-separated list of package formats to create')
    parser.add_argument(
        '--external',
        type=str,
        default=[],
        nargs='+',
        help=('space-separated list of paths to files that are to be included '
              'in the package'))
    opts = parser.parse_args()
    create_packages(build_dir=opts.build_dir,
                    formats=opts.formats,
                    external=opts.external)


if __name__ == '__main__':
    main()
