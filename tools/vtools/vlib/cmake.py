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


def configure_build(vconfig, build_external=True, build_external_only=False):

    if vconfig.compiler == 'clang':
        clang_path = clang.find_or_install_clang(vconfig)
        os.environ['CC'] = clang_path
        os.environ['CXX'] = f'{clang_path}++'

    logging.info(f"Configuring '{vconfig.build_type}' build.")

    cmake_flags = [
        f'-DV_DEPS_INSTALL_DIR={vconfig.external_path}',
        f'-DCMAKE_BUILD_TYPE={vconfig.build_type.capitalize()} '
    ]

    # change value of default cmake config options based on given args. Form
    # more on what these do, take a look at /v/CMakeLists.txt
    if not build_external:
        cmake_flags.append('-DV_DEPS_SKIP_BUILD=ON')
    if build_external_only:
        cmake_flags.append('-DV_DEPS_ONLY=ON')
    if vconfig.compiler == 'clang':
        cmake_flags.append('-DRP_ENABLE_GOLD_LINKER=OFF')

    os.makedirs(vconfig.build_dir, exist_ok=True)

    shell.run_subprocess(f'cmake -GNinja'
                         f'  {" ".join(cmake_flags)}'
                         f'  -B{vconfig.build_dir}'
                         f'  -H{vconfig.src_dir}')

    # FIXME https://app.asana.com/0/1149841353291489/1153763539998305
    if build_external:
        src = (f'{vconfig.build_dir}/v_deps_build/seastar-prefix/'
               f'src/seastar-build/apps/iotune/iotune')
        dst = f'{vconfig.external_path}/bin/iotune'
        if os.path.isfile(dst):
            os.remove(dst)
        shutil.move(src, dst)


def _cache_file(vconfig):
    return f'{vconfig.build_dir}/CMakeCache.txt'
