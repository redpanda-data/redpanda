#!/usr/bin/env python3
import sys
import os
import logging
import re
import tarfile
import subprocess
import io
import functools
import shutil

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

from constants import *
from pkg_config import *
import shell

thunk = b'''\
#!/bin/bash

command="$(readlink -f "$0")"
basename="$(basename "$command")"
directory="$(dirname "$command")/.."
ldso="$directory/libexec/$basename"
realexe="$directory/libexec/$basename.bin"
LD_LIBRARY_PATH="$directory/lib" exec -a "$0" "$ldso" "$realexe" "$@"
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


def relocable_tar_package(dest, execs, configs):
    logger.info("Creating relocable tar package %s", dest)
    gzip_process = subprocess.Popen(
        "pigz -f > %s" % dest, shell=True, stdin=subprocess.PIPE)
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
    ar.close()
    gzip_process.communicate()


def _in_dist_root(path):
    return os.path.join(RP_DIST_ROOT, path)


def _in_root(path):
    return os.path.join(RP_ROOT, path)


def red_panda_tar(input_tar):
    logger.info("Creating tarball package")
    tar_dir = _in_dist_root('tar')
    os.makedirs(tar_dir, exist_ok=True)
    tar_name = 'redpanda-%s-%s_%s.tar.gz' % (VERSION, RELEASE, REVISION)
    tar_file = os.path.join(tar_dir, tar_name)
    shutil.copy(input_tar, tar_file)
