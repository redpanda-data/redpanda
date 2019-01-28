#!/usr/bin/env python3
import sys
import os
import logging
import urllib
import urllib.request
import stat

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

from constants import *
import shell
import git

def _get_clang_prog(prog):
    """ALL tools for clang should be pinned to one version"""
    ret = shell.run_oneline("which %s" % prog)
    if ret != None:
        if shell.run_oneline(
                "%s --version | grep %s | awk '{print $3}'" %
            (ret, CLANG_SOURCE_VERSION)) != CLANG_SOURCE_VERSION:
            return None
        return ret
    return None


def get_clang_format():
    return _get_clang_prog("clang-format")


def get_clang_tidy():
    return _get_clang_prog("clang-tidy")


def check_bdir():
    if not os.path.exists(RP_BUILD_ROOT): os.makedirs(RP_BUILD_ROOT)


def get_cpplint():
    check_bdir()
    linter = "%s/%s" % (RP_BUILD_ROOT, "cpplint.py")
    if not os.path.exists(linter):
        urllib.request.urlretrieve(CPPLINT_URL, linter)
        s = os.stat(linter)
        os.chmod(linter, s.st_mode | stat.S_IEXEC)
    return linter


def get_smf_install_deps():
    _check_bdir()
    deps = "%s/%s" % (RP_BUILD_ROOT, "smf_install_deps.sh")
    if not os.path.exists(deps):
        urllib.request.urlretrieve(INSTALL_DEPS_URL, deps)
    return deps
