#!/usr/bin/env python3
import sys
import os
import logging
import glob
from string import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

from constants import *
import git
import golang
import shell
import fmt
import cpp


def install_deps():
    logger.info("Checking for deps scripts")
    cpp.get_smf_install_deps()
    logger.info("installing deps")
    user_id = shell.run_oneline("id -u")

    shell.run_subprocess(
        "%s bash %s/%s" % ('' if user_id == '0' else 'sudo', RP_BUILD_ROOT,
                           "smf_install_deps.sh"))
    shell.run_subprocess("%s bash %s/tools/install-deps.sh" %
                         ('' if user_id == '0' else 'sudo', RP_ROOT))


def _check_build_type(build_type):
    if build_type not in ["debug", "release"]:
        raise Exception("Build type is neither release or debug or all")


def _symlink_compile_commands(build_type):
    cpp.check_bdir()
    src = "%s/%s/compile_commands.json" % (RP_BUILD_ROOT, build_type)
    dst = "%s/compile_commands.json" % RP_ROOT
    if os.path.islink(dst): os.unlink(dst)
    os.symlink(src, dst)


def _configure_build(build_type):
    _check_build_type(build_type)
    cpp.check_bdir(build_type)
    logger.info("configuring build %s" % build_type)
    tpl = Template(
        "cd $build_root/$build_type && cmake -GNinja -DCMAKE_BUILD_TYPE=$cmake_type $root"
    )
    cmd = tpl.substitute(
        root=RP_ROOT,
        build_root=RP_BUILD_ROOT,
        cmake_type=build_type.capitalize(),
        build_type=build_type)
    shell.run_subprocess(cmd)


def _invoke_build(build_type):
    _check_build_type(build_type)
    tpl = Template(
        "cd $build_root/$build_type && ninja -C $build_root/$build_type")
    cmd = tpl.substitute(
        root=RP_ROOT, build_root=RP_BUILD_ROOT, build_type=build_type)
    shell.run_subprocess(cmd)
    _symlink_compile_commands(build_type)


def _invoke_tests(build_type):
    _check_build_type(build_type)
    rp_test_regex = "\".*_rp(unit|bench|int)$\""
    tpl = Template("cd $build_root/$build_type && ctest -R $re")
    cmd = tpl.substitute(
        build_root=RP_BUILD_ROOT, re=rp_test_regex, build_type=build_type)
    shell.run_subprocess(cmd)


def _invoke_build_go_cmds():
    for cmd_path in glob.glob("%s/*/*.go" % GOLANG_CMDS_ROOT):
        cmd_dir, cmd_main = os.path.split(cmd_path)
        _, cmd_name = os.path.split(cmd_dir)
        logger.info("Building %s...", cmd_name)
        golang.go_build(cmd_dir, cmd_main,
                        "%s/%s" % (GOLANG_BUILD_ROOT, cmd_name))


def _invoke_go_tests():
    golang.go_test(GOLANG_ROOT, "./...")


def build(build_type, targets):
    if 'all' in targets or 'go' in targets:
        _invoke_go_tests()
        _invoke_build_go_cmds()

    if 'all' in targets or 'cpp' in targets:
        logger.info("Building Cpp...")
        _configure_build(build_type)
        _invoke_build(build_type)
        _invoke_tests(build_type)
