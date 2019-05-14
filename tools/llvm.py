#!/usr/bin/env python3
import sys
import os
import logging
import re
import tarfile
import subprocess
import io
import functools
import requests
import hashlib
import glob

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

from constants import *
import shell
import clang


def get_llvm():
    if not _check_llvm():
        _download_and_check_llvm_sources()


def build_llvm():
    os.makedirs(_get_llvm_build_path(), exist_ok=True)
    os.makedirs(get_llvm_install_path(), exist_ok=True)
    cmake_tmpl = 'cd {build_root} && cmake -G Ninja  -C {llvm_cmake_cache} {args} {src_root}/llvm'
    logger.info("Configuring LLVM bootstrap build....")
    shell.run_subprocess(
        cmake_tmpl.format(
            build_root=_get_llvm_build_path(),
            llvm_cmake_cache=_get_llvm_cmake_path(),
            args=_join_cmake_args(),
            src_root=_get_llvm_src_path()))
    logger.info("Building LLVM...")
    ninja_tmpl = 'cd {build_root} && ninja stage2 && ninja stage2-install'
    shell.run_subprocess(ninja_tmpl.format(build_root=_get_llvm_build_path()))


def _check_llvm():
    llvm_path = _get_llvm_src_path()
    return os.path.isdir(llvm_path) and os.path.isdir(
        os.path.join(llvm_path, 'libcxx')) and os.path.isdir(
            os.path.join(llvm_path, 'libcxxabi'))


def _download_and_check_llvm_sources():
    os.makedirs(os.path.join(RP_BUILD_ROOT, 'llvm'), exist_ok=True)
    llvm_src_url = 'https://github.com/llvm/llvm-project/archive/%s.tar.gz' % LLVM_REF
    logger.info("Downloading LLVM sources from %s" % llvm_src_url)
    resp = requests.get(llvm_src_url)
    io_bytes = io.BytesIO(resp.content)
    downloaded_digest = hashlib.md5(io_bytes.getbuffer()).hexdigest()
    if downloaded_digest != LLVM_MD5:
        logger.error(
            "LLVM tarbal MD5 checksum mismatch - wanted '%s', downloaded '%s'",
            LLVM_MD5, downloaded_digest)
        raise RuntimeError(
            "LLVM tarbal MD5 checksum mismatch expected md5sum %s but got %s" %
            (LLVM_MD5, downloaded_digest))
    tar = tarfile.open(fileobj=io_bytes, mode='r')

    logger.info("Extracting LLVM tarball...")
    tar.extractall(path=RP_BUILD_ROOT)
    llvm_dir = os.path.join(RP_BUILD_ROOT, "llvm-project-%s" % LLVM_REF)
    os.rename(llvm_dir, _get_llvm_src_path())


def _get_llvm_src_path():
    return os.path.join(RP_BUILD_ROOT, 'llvm', 'llvm-src')


def _get_llvm_build_path():
    return os.path.join(RP_BUILD_ROOT, 'llvm', 'llvm-build')


def get_llvm_install_path():
    return os.path.join(RP_BUILD_ROOT, 'llvm', 'llvm-bin')


def _get_llvm_cmake_path():
    # All the LLVM bootstrap build conifguration can be set in
    # <root>/cmake/llvm.cmake
    return os.path.join(RP_ROOT, "cmake", "caches", "llvm.cmake")


def _llvm_cmake_args():
    return {
        'CMAKE_INSTALL_PREFIX': get_llvm_install_path(),
    }


def _join_cmake_args():
    return ' '.join(
        map(lambda kv: '-D%s=\"%s\"' % (kv[0], kv[1]),
            _llvm_cmake_args().items()))
