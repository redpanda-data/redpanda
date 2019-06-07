#!/usr/bin/env python3
import sys
import os
import logging
import glob
from string import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

from constants import *
from pkg_config import *
import git
import golang
import shell
import fmt
import cpp
import clang
import llvm
import packaging


def _check_build_type(build_type):
    if build_type not in ["debug", "release"]:
        raise Exception("Build type is neither release or debug or all")


def _symlink_compile_commands(build_type):
    cpp.check_bdir()
    src = "%s/%s/compile_commands.json" % (RP_BUILD_ROOT, build_type)
    dst = "%s/compile_commands.json" % RP_ROOT
    if os.path.islink(dst): os.unlink(dst)
    os.symlink(src, dst)


def _configure_build(build_type, clang_opt):
    _check_build_type(build_type)
    cpp.check_bdir(build_type)
    logger.info("configuring build %s" % build_type)
    tpl = Template(
        "cd $root && cmake -GNinja -DCMAKE_BUILD_TYPE=$cmake_type "
        "-B$build_root/$build_type -H$root"
    )
    build_env = os.environ
    if clang_opt == None:
        logger.debug(
            "Clang not defined, building using default system compiler")
    elif clang_opt == "internal":
        logger.info("Builing using internal Clang compiler")
        llvm.get_llvm()
        llvm.build_llvm()
        build_env = clang.clang_env_from_path(
            os.path.join(llvm.get_llvm_install_path(), "bin", "clang"))
    else:
        logger.info("Using clang compiler from path `%s`" % clang_opt)
        build_env = clang.clang_env_from_path(clang_opt)
    cmd = tpl.substitute(root=RP_ROOT,
                         build_root=RP_BUILD_ROOT,
                         cmake_type=build_type.capitalize(),
                         build_type=build_type)
    shell.run_subprocess(cmd, build_env)


def _invoke_build(build_type):
    _check_build_type(build_type)
    tpl = Template(
        "cd $build_root/$build_type && ninja -C $build_root/$build_type")
    cmd = tpl.substitute(root=RP_ROOT,
                         build_root=RP_BUILD_ROOT,
                         build_type=build_type)
    shell.run_subprocess(cmd)
    _symlink_compile_commands(build_type)


def _invoke_tests(build_type):
    _check_build_type(build_type)
    rp_test_regex = "\".*_rp(unit|bench|int)$\""
    tpl = Template("cd $build_root/$build_type && ctest -R $re")
    cmd = tpl.substitute(build_root=RP_BUILD_ROOT,
                         re=rp_test_regex,
                         build_type=build_type)
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


def build(build_type, targets, clang):
    if 'all' in targets or 'go' in targets:
        _invoke_go_tests()
        _invoke_build_go_cmds()

    if 'all' in targets or 'cpp' in targets:
        logger.info("Building Cpp...")
        _configure_build(build_type, clang)
        _invoke_build(build_type)
        _invoke_tests(build_type)


def build_packages(build_type, packages):
    res_type = "release" if build_type == "none" else build_type
    if packages:
        execs = [
            "%s/%s/src/v/redpanda/redpanda" % (RP_BUILD_ROOT, res_type),
            "%s/%s/v_deps_build/seastar-prefix/src/seastar-build/apps/iotune/iotune"
            % (RP_BUILD_ROOT, res_type),
            "%s/go/bin/rpk" % RP_BUILD_ROOT,
            "%s/%s/v_deps_install/bin/hwloc-calc" % (RP_BUILD_ROOT, res_type),
            "%s/%s/v_deps_install/bin/hwloc-distrib" % (RP_BUILD_ROOT, res_type),
        ]
        configs = [os.path.join(RP_ROOT, "conf/redpanda.yaml")]
        os.makedirs(RP_DIST_ROOT, exist_ok=True)
        tar_name = 'redpanda.tar.gz'
        tar_path = "%s/%s" % (RP_DIST_ROOT, tar_name)
        packaging.relocable_tar_package(tar_path, execs, configs)
        if 'tar' in packages:
            packaging.red_panda_tar(tar_path)
        if 'deb' in packages:
            packaging.red_panda_deb(tar_path)
        if 'rpm' in packages:
            packaging.red_panda_rpm(tar_path)
        os.remove(tar_path)
