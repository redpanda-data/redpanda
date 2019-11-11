#!/usr/bin/env python3
import math
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


def _configure_build(build_type, external, external_only,
                     external_install_prefix, clang_opt):
    _check_build_type(build_type)
    cpp.check_bdir(build_type)
    logger.info("configuring build %s" % build_type)
    args = []
    tpl = Template(
        "cd $root && cmake -GNinja $args -DCMAKE_BUILD_TYPE=$cmake_type "
        "-B$build_root/$build_type -H$root")
    build_env = os.environ
    if clang_opt:
        args.append('-DRP_ENABLE_GOLD_LINKER=OFF')
    if clang_opt == None:
        logger.debug(
            "Clang not defined, building using default system compiler")
    elif clang_opt == "internal" or clang_opt == "llvm_bootstrap":
        logger.info("Builing using internal Clang compiler")
        llvm.get_llvm()
        bootstrap_build = clang_opt == "llvm_bootstrap"
        llvm.build_llvm(bootstrap_build, external_install_prefix)
        clang_bin_path = os.path.join(
            external_install_prefix if external_install_prefix else
            llvm.get_internal_llvm_install_path(), 'bin', 'clang')
        build_env = clang.clang_env_from_path(clang_bin_path)
    else:
        logger.info("Using clang compiler from path `%s`" % clang_opt)
        build_env = clang.clang_env_from_path(clang_opt)

    if not external:
        args.append('-DV_MANAGE_DEPS=OFF')
    if external_only:
        args.append('-DV_DEPS_ONLY=ON')
    if external_install_prefix:
        args.append('-DV_DEPS_INSTALL_DIR=%s' % external_install_prefix)

    cmd = tpl.substitute(root=RP_ROOT,
                         build_root=RP_BUILD_ROOT,
                         args=' '.join(args),
                         cmake_type=build_type.capitalize(),
                         build_type=build_type)
    shell.run_subprocess(cmd, build_env)


def _invoke_build(build_type):
    _check_build_type(build_type)
    tpl = Template(
        "cd $build_root/$build_type && ninja -C $build_root/$build_type -j$num_jobs")

    # assign jobs so that we have 2.0GB/core
    total_memory = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    num_jobs = math.floor(total_memory / (2 * 1024.**3))
    cmd = tpl.substitute(root=RP_ROOT,
                         build_root=RP_BUILD_ROOT,
                         build_type=build_type,
                         num_jobs=num_jobs)
    shell.run_subprocess(cmd)
    _symlink_compile_commands(build_type)


def _invoke_tests(build_type):
    _check_build_type(build_type)
    rp_test_regex = "\".*_rp(unit|bench|int)$\""
    tpl = Template("cd $build_root/$build_type && ctest $verbose -R $re")
    cmd = tpl.substitute(build_root=RP_BUILD_ROOT,
                         re=rp_test_regex,
                         build_type=build_type,
                         verbose="-V" if os.environ.get("CI") else "")
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


def build(build_type, targets, external, external_only,
          external_install_prefix, clang):
    if 'all' in targets or 'go' in targets:
        _invoke_go_tests()
        _invoke_build_go_cmds()

    if 'all' in targets or 'cpp' in targets:
        logger.info("Building Cpp...")
        _configure_build(build_type, external, external_only,
                         external_install_prefix, clang)
        if external_only:
            return
        _invoke_build(build_type)
        _invoke_tests(build_type)


def build_packages(build_type, packages):
    if not packages:
        return

    res_type = "release" if build_type == "none" else build_type

    build_dir = "%s/%s/" % (RP_BUILD_ROOT, res_type)

    execs = [
        "%s/v_deps_build/seastar-prefix/src/seastar-build/apps/iotune/iotune" %
        build_dir,
        "%s/v_deps_install/bin/hwloc-calc" % build_dir,
        "%s/v_deps_install/bin/hwloc-distrib" % build_dir,
    ]

    packaging.create_packages(packages,
                              build_dir=RP_BUILD_ROOT,
                              build_type=res_type,
                              external=execs)
