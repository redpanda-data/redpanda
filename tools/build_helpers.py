#!/usr/bin/env python3
import sys
import os
import logging
from string import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')


from constants import *
import git
import shell
import fmt
import cpp

def install_deps():
    logger.info("Checking for deps scripts")
    cpp.get_smf_install_deps()
    logger.info("installing deps")
    shell.run_subprocess(
        "sudo sh %s/%s" % (RP_BUILD_ROOT, "smf_install_deps.sh"))


def _check_build_type(build_type):
    if build_type not in ["debug", "release"]:
        raise Exception("Build type is neither release or debug or all")


def _symlink_compile_commands(x):
    cpp.check_bdir()
    src = "%s/%s/compile_commands.json" % (RP_BUILD_ROOT, x)
    dst = "%s/compile_commands.json" % RP_ROOT
    if os.path.islink(dst): os.unlink(dst)
    os.symlink(src, dst)


def _configure_build(build_type):
    _check_build_type(build_type)
    cpp.check_bdir()
    logger.info("configuring build %s" % build_type)
    if build_type in ["debug"]:
        cmd = "sh %s/cooking.sh -r wellknown -t Debug" % RP_ROOT
        shell.run_subprocess("cd %s && %s " % (RP_ROOT, cmd))
    elif build_type in ["release"]:
        cmd = "sh %s/cooking.sh -r wellknown -t Release" % RP_ROOT
        shell.run_subprocess("cd %s && %s " % (RP_ROOT, cmd))


def _invoke_build(build_type):
    _check_build_type(build_type)
    shell.run_subprocess("cd %s && ninja -C %s" % (RP_ROOT, RP_BUILD_ROOT))


def _invoke_tests(build_type):
    _check_build_type(build_type)
    rp_test_regex = "\".*_rp(unit|bench|int)$\""
    shell.run_subprocess(
        "cd %s && ctest -R %s" % (RP_BUILD_ROOT, rp_test_regex))


def build(build_type):
    _configure_build(build_type)
    _invoke_build(build_type)
    _invoke_tests(build_type)
