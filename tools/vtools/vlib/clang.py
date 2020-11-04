import hashlib
import io
import os
import requests
import shutil
import tarfile

from absl import logging
from . import shell

LLVM_GITREF = 'llvmorg-11.0.0'
LLVM_TARBALL_MD5SUM = '1f9f15cb95a405610499be15093f136c'


def find_or_install_clang(vconfig):
    """. Probes the `vconfig.clang_path` property and immediately returns
    `None` if its value is `None`. Otherwise, it attempts to find clang
    binaries on that path and, if not found, installs the clang compiler and
    places it on that location.
    """
    if not vconfig.clang_path:
        # no 'clang_path' defined => return immediately
        return None

    for p in ['llvm-bin', 'bin', '.']:
        clang_path = f'{vconfig.clang_path}/{p}/clang'
        logging.debug(f'Looking for existing clang in {clang_path}')
        if os.path.isfile(clang_path):
            return clang_path

    # clang not found but 'clang_path' is defined, so let's build it
    install_clang(vconfig)

    return f"{vconfig.clang_path}/bin/clang"


def install_clang(vconfig, download_only=False):
    llvm_root = f"{vconfig.build_root}/llvm"
    src_dir = f"{llvm_root}/llvm-src"

    if os.path.isdir(f"{src_dir}/clang"):
        logging.info(f"Found source in {src_dir}, skipping download.")
    else:
        _download_checksum_and_extract_llvm_sources(src_dir)

    if download_only:
        return

    build_dir = f"{llvm_root}/llvm-build"
    install_prefix = vconfig.clang_path
    llvm_cache_file = f"{vconfig.src_dir}/cmake/caches/llvm.cmake"

    _build_clang(src_dir, build_dir, llvm_cache_file, install_prefix,
                 vconfig.environ)


def _build_clang(src_dir, build_dir, llvm_cache_file, install_prefix, env):
    if os.path.exists(f'{install_prefix}/bin/clang'):
        logging.info(f"clang exists: {install_prefix}/bin/clang")
        return
    os.makedirs(build_dir, exist_ok=True)
    logging.info("Configuring LLVM build....")
    shell.run_subprocess(
        f'cd {build_dir} && '
        f'cmake -G Ninja '
        f'  -C {llvm_cache_file} '
        f'  -DCMAKE_INSTALL_PREFIX={install_prefix}'
        f'  -DLIBCXX_CXX_ABI_INCLUDE_PATHS={src_dir}/libcxxabi/include'
        f'  -DLIBCXX_CXX_ABI_LIBRARY_PATH={install_prefix}/lib'
        f' {src_dir}/llvm',
        env=env)
    logging.info("Building LLVM...")
    shell.run_subprocess(
        f'cd {build_dir} && ninja install-cxxabi && ninja install', env=env)
    shutil.copyfile(f'{build_dir}/lib/libc++experimental.a',
                    f'{install_prefix}/lib/libc++experimental.a')


def _download_checksum_and_extract_llvm_sources(src_dir):
    os.makedirs(src_dir, exist_ok=True)
    llvm_src_url = (
        f'https://github.com/llvm/llvm-project/archive/{LLVM_GITREF}.tar.gz')
    logging.info(f'Downloading LLVM sources from {llvm_src_url}')
    resp = requests.get(llvm_src_url)
    io_bytes = io.BytesIO(resp.content)
    downloaded_digest = hashlib.md5(io_bytes.getbuffer()).hexdigest()
    if downloaded_digest != LLVM_TARBALL_MD5SUM:
        logging.fatal("LLVM tarball MD5 checksum mismatch - wanted")
    tar = tarfile.open(fileobj=io_bytes, mode='r')

    logging.info("Extracting LLVM tarball...")
    tar.extractall(path=os.path.dirname(src_dir))
    llvm_dir = os.path.join(os.path.dirname(src_dir),
                            "llvm-project-" + LLVM_GITREF)
    os.rename(llvm_dir, src_dir)
