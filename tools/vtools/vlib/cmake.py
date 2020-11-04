import os
import shutil

from absl import logging
from . import shell
from . import clang


def cache_exists(vconfig):
    return os.path.exists(_cache_file(vconfig))


def rm_cache(vconfig):
    if cache_exists(vconfig):
        os.remove(_cache_file(vconfig))


def _render_build_script(env, cmake_str, build_dir):
    nl = "\n"
    tpl = f"""#!/bin/env bash
set -x
{nl.join(["export %s=%s" % (k, v) for k, v in env.items()])}

{cmake_str}
"""
    with open(f"{build_dir}/rebuild.sh", 'w') as f:
        os.fchmod(f.fileno(), 0o744)
        f.write(tpl)


def configure_build(vconfig,
                    build_external=True,
                    build_external_only=False,
                    enable_dpdk=False):
    if vconfig.compiler == 'clang':
        clang_path = clang.find_or_install_clang(vconfig)
        vconfig.environ['CC'] = clang_path
        vconfig.environ['CXX'] = f'{clang_path}++'
    else:
        vconfig.environ['CC'] = '/usr/bin/gcc'
        vconfig.environ['CXX'] = '/usr/bin/g++'

    logging.info(f"Configuring '{vconfig.build_type}' build.")

    cmake_flags = [
        f'-DREDPANDA_DEPS_INSTALL_DIR={vconfig.external_path}',
        f'-DCMAKE_BUILD_TYPE={vconfig.build_type.capitalize()} ',
        f'-DCMAKE_C_COMPILER={vconfig.environ["CC"]} ',
        f'-DCMAKE_CXX_COMPILER={vconfig.environ["CXX"]} '
    ]
    if enable_dpdk:
        cmake_flags.append("-DRP_ENABLE_DPDK=ON")

    # change value of default cmake config options based on given args. Form
    # more on what these do, take a look at /v/CMakeLists.txt
    if not build_external:
        cmake_flags.append('-DV_DEPS_SKIP_BUILD=ON')
    if build_external_only:
        cmake_flags.append('-DV_DEPS_ONLY=ON')

    os.makedirs(vconfig.build_dir, exist_ok=True)
    cmake_str = (f'cmake -GNinja'
                 f'  {" ".join(cmake_flags)}'
                 f'  -B{vconfig.build_dir}'
                 f'  -H{vconfig.src_dir}')
    _render_build_script(vconfig.environ, cmake_str, vconfig.build_dir)
    shell.run_subprocess(f"sh {vconfig.build_dir}/rebuild.sh",
                         env=vconfig.environ)


def _cache_file(vconfig):
    return f'{vconfig.build_dir}/CMakeCache.txt'
