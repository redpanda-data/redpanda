import git
import glob
import io
import os
import pathlib
import lddwrap
import shutil
import subprocess
import tarfile

from absl import logging
from . import fs
from . import shell
from . import templates

VENDOR = "Vectorized Inc."
PRODUCT = {
    "redpanda": {
        "DESCRIPTION": "Redpanda, the fastest queue in the West",
        "CATEGORY": "Applications/Misc",
        "URL": "https://vectorized.io/product",
        "CODENAME": "pandacub"
    },
    "pandaproxy": {
        "DESCRIPTION": "Pandaproxy, a REST API for Kafka",
        "CATEGORY": "Applications/Misc",
        "URL": "https://vectorized.io/product",
        "CODENAME": "sleepycub"
    }
}
LICENSE = 'Proprietary and confidential'
VERSION = "0.0"
REVISION = "0000000"
RELEASE = "1"


def _get_dependencies(binary, vconfig):
    logging.debug(f"Getting dependencies of {binary}")

    if vconfig.compiler == 'clang':
        libasan_path = shell.run_oneline(
            f'clang -print-file-name=libclang_rt.asan-x86_64.so',
            env=vconfig.environ)
    else:
        libasan_path = shell.run_oneline('gcc -print-file-name=libasan.so',
                                         env=vconfig.environ)
    compiler_libs_path = os.path.dirname(libasan_path)

    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = (f'{vconfig.external_path}/lib:'
                              f'{vconfig.external_path}/lib64:'
                              f'{compiler_libs_path}')
    deps = lddwrap.list_dependencies(path=pathlib.Path(binary), env=env)

    libs = {}
    for dep in deps:
        if not dep.found:
            logging.fatal(f"Cannot find location for {dep.soname}")

        if not dep.soname:
            # linux libs have soname=None
            if 'ld-' in str(dep.path):
                # we ignore all but the loader
                libs['ld.so'] = os.path.realpath(dep.path)
            continue

        libs[dep.soname] = os.path.realpath(dep.path)

    return libs


def _relocatable_dir(dest_dir, execs, configs, admin_api_swag, api_swag,
                     vconfig):
    """
    Create a directory containing package artifacts suitable for relocation on
    the local machine, including within a docker container. This primarily used
    by test harnesses, and the goal is to be a fast operation.

       1. Executables are patched and copied into the staging directory
       2. System libraries are copied into the staging directory
       3. All other dependencies under the build root are symlinked
       4. Executables and libraries are copied only if they have changed

    Together this reduces from scratch packaging times to around a couple
    seconds compared to around ten for building a tarball. When no changes have
    been detected the command runs in less than one second.

    The resulting directory can be run from any location within the docker
    container provided that the original build root path is preserved in the
    container to resolve symlinks.
    """
    logging.info(f"staging relocatable local dir at {dest_dir}")

    for name in [
            "lib", "libexec", "conf", "bin",
            f"etc/{vconfig.product}/admin-api-doc",
            f"etc/{vconfig.product}/api-doc"
    ]:
        os.makedirs(os.path.join(dest_dir, name), exist_ok=True)

    def is_newer(path_a, path_b):
        # returns true if path_b doesn't exist or path_a is newer. the
        # action is also executed if the file extra doesn't exist, which is used
        # to deal with derived files like thunks.
        src_mtime = os.stat(path_a).st_mtime
        dest_mtime = 0
        try:
            dest_mtime = os.stat(path_b).st_mtime
            if src_mtime <= dest_mtime:
                return False
        except FileNotFoundError:
            pass
        return True

    def maybe_symlink(target, link):
        exists = os.path.exists(link)
        if not exists or not os.path.samefile(target, os.path.realpath(link)):
            logging.debug("Creating symlink %s -> %s", link, target)
            if exists:
                os.remove(link)
            os.symlink(target, link)

    def thunk_path(exe):
        return os.path.join(dest_dir, "bin", os.path.basename(exe))

    def patch_exe(src, dest):
        shutil.copy2(src, dest)
        _patch_exe(dest_dir, dest, vconfig.environ)
        path = thunk_path(dest)
        with open(path, 'w') as f:
            thunk = _render_thunk(dest_dir, os.path.basename(dest))
            f.write(thunk)
        os.chmod(path, 0o755)

    libs = {}
    manifest = set()

    for exe in execs:
        dest_exe = os.path.join(dest_dir, "libexec", exe.mangled_name())
        thunk = thunk_path(exe.mangled_name())
        if is_newer(exe.path, dest_exe):
            patch_exe(exe.path, dest_exe)
        libs.update(_get_dependencies(exe.path, vconfig))
        manifest.add(dest_exe)
        manifest.add(thunk)

    for name, path in libs.items():
        basedir = os.path.commonpath((path, vconfig.build_root))
        if not os.path.samefile(basedir, vconfig.build_root):
            dest_lib = os.path.join(dest_dir, "lib", name)
            if is_newer(path, dest_lib):
                shutil.copy2(path, dest_lib)
            manifest.add(dest_lib)

    for conf in configs:
        dest_path = os.path.join(dest_dir, "conf", os.path.basename(conf))
        manifest.add(dest_path)
        maybe_symlink(conf, dest_path)

    for swag in admin_api_swag:
        dest_path = os.path.join(dest_dir,
                                 f"etc/{vconfig.product}/admin-api-doc",
                                 os.path.basename(swag))
        manifest.add(dest_path)
        maybe_symlink(swag, dest_path)

    for swag in api_swag:
        dest_path = os.path.join(dest_dir, f"etc/{vconfig.product}/api-doc",
                                 os.path.basename(swag))
        manifest.add(dest_path)
        maybe_symlink(swag, dest_path)

    # remove old staged files, if any
    for root, _, files in os.walk(dest_dir):
        for filename in files:
            path = os.path.join(root, filename)
            if path not in manifest:
                logging.debug("Removing staged file not in manifest: %s", path)
                os.remove(path)


def _relocable_tar_package(dest, execs, configs, admin_api_swag, api_swag,
                           vconfig):
    rp_root = f'/opt/{vconfig.product}'
    logging.info(f"staging relocatable tar package {dest}")
    gzip_process = subprocess.Popen(f"pigz -f > {dest}",
                                    shell=True,
                                    env=vconfig.environ,
                                    stdin=subprocess.PIPE)

    ar = tarfile.open(fileobj=gzip_process.stdin, mode='w|')
    all_libs = {}
    for exe in execs:
        # Make a copy of the exe and patch it.
        patched_exe = f'{exe.path}.patched'
        shutil.copy(exe.path, patched_exe)
        # We need to patch the executable, overwriting its requested interpreter
        # with the one shipped with redpanda (/opt/redpanda/lib/ld.so),
        # so that gdb can load the debug symbols when debugging redpanda.
        # https://github.com/scylladb/scylla/issues/4673
        _patch_exe(rp_root, patched_exe, vconfig.environ)

        logging.debug(
            f"Adding '{exe.path}' executable to relocable tar as {exe.mangled_name()}")
        ar.add(patched_exe, arcname=f'libexec/{exe.mangled_name()}')
        thunk = _render_thunk(rp_root, exe.mangled_name())
        ti = tarfile.TarInfo(name=f'bin/{exe.mangled_name()}')
        ti.size = len(thunk)
        ti.mode = 0o755
        ti.mtime = os.stat(patched_exe).st_mtime
        ar.addfile(ti, fileobj=io.BytesIO(thunk.encode()))
        all_libs.update(_get_dependencies(exe.path, vconfig))

    for lib, location in all_libs.items():
        logging.debug(f"Adding '{location}' lib to relocatable tar")
        ar.add(location, arcname=f'lib/{lib}')

    for conf in configs:
        ar.add(conf, arcname=f"conf/{os.path.basename(conf)}")

    for swag in admin_api_swag:
        arcname = f"etc/{vconfig.product}/admin-api-doc/{os.path.basename(swag)}"
        ar.add(swag, arcname=arcname)

    for swag in api_swag:
        arcname = f"etc/{vconfig.product}/api-doc/{os.path.basename(swag)}"
        ar.add(swag, arcname=arcname)

    ar.close()
    gzip_process.communicate()


def product_tar(product, input_tar, dest_path):
    logging.info("Creating tarball package")
    tar_dir = os.path.join(dest_path, "tar")
    os.makedirs(tar_dir, exist_ok=True)
    tar_name = f'{product}-{VERSION}-{RELEASE}_{REVISION}.tar.gz'
    tar_file = os.path.join(tar_dir, tar_name)
    logging.info(f"Final tarball: {tar_file}")
    shutil.copy(input_tar, tar_file)


def _rpm_tree(dest):
    for dir in ['BUILD', 'BUILDROOT', 'RPMS', 'SOURCES', 'SPECS', 'SRPMS']:
        os.makedirs(f"{dest}/{dir}", exist_ok=True)


def _pkg_context(product):
    return {
        "name": product,
        "version": VERSION,
        "summary": PRODUCT[product]["DESCRIPTION"],
        "desc": PRODUCT[product]["DESCRIPTION"],
        "release": RELEASE,
        "license": LICENSE,
        "revision": REVISION,
        "codename": PRODUCT[product]["CODENAME"]
    }


def red_panda_rpm(input_tar, dest_path, src_dir, env):
    logging.info("Creating RPM package")
    # prepare RPM sources
    rpm_tree_root = os.path.join(dest_path, "rpm")
    shutil.rmtree(rpm_tree_root, ignore_errors=True)
    _rpm_tree(os.path.join(dest_path, "rpm"))
    fs.force_link(input_tar, os.path.join(dest_path,
                                          "rpm/SOURCES/redpanda.tar"))
    shutil.copytree(f'{src_dir}/packaging/common',
                    os.path.join(dest_path, "rpm/common"),
                    ignore=_is_template)
    # render templates
    package_ctx = _pkg_context("redpanda")
    package_ctx['source_tar'] = "redpanda.tar"
    spec_template = f"{src_dir}/packaging/rpm/redpanda.spec.j2"
    spec = os.path.join(dest_path, "rpm/SPECS/redpanda.spec")
    templates.render_to_file(spec_template, spec, package_ctx)
    _render_systemd_templates(os.path.join(dest_path, "rpm/common"),
                              {'redhat': True}, src_dir)
    # build RPM
    nproc = os.cpu_count()
    shell.run_subprocess(
        f'rpmbuild -bb --define \"_topdir {rpm_tree_root}\" --define \"_binary_payload w2T{nproc}.xzdio\" {spec}',
        env=env)


def red_panda_deb(input_tar, dest_path, src_dir, env):
    logging.info("Creating DEB package")
    debian_dir = os.path.join(dest_path, "debian/redpanda")
    os.makedirs(debian_dir, exist_ok=True)
    shutil.rmtree(debian_dir)
    shutil.copytree(f"{src_dir}/packaging/debian/debian",
                    os.path.join(debian_dir, 'debian'))
    target_tar_name = f"debian/redpanda_{VERSION}-{RELEASE}.orig.tar.gz"
    fs.force_link(input_tar, os.path.join(dest_path, target_tar_name))
    common_path = os.path.join(dest_path, "debian/redpanda/common")
    shutil.copytree(f'{src_dir}/packaging/common',
                    common_path,
                    ignore=_is_template)

    # render templates
    package_ctx = _pkg_context("redpanda")
    chglog_tmpl = f"{src_dir}/packaging/debian/changelog.j2"
    control_tmpl = f"{src_dir}/packaging/debian/control.j2"
    _render_systemd_templates(common_path, {"debian": True}, src_dir)
    for f in glob.glob(os.path.join(common_path, "systemd", "*")):
        shutil.copy(f, os.path.join(dest_path, "debian/redpanda/debian"))
    templates.render_to_file(
        chglog_tmpl, os.path.join(dest_path,
                                  "debian/redpanda/debian/changelog"),
        package_ctx)
    templates.render_to_file(
        control_tmpl,
        os.path.join(dest_path, "debian/redpanda/debian/control"), package_ctx)
    # build DEB
    shell.raw_check_output(f"tar -C {debian_dir} -xpf {input_tar}", env=env)
    shell.run_subprocess(f'cd {debian_dir} && debuild -rfakeroot -us -uc -b',
                         env=env)


def _is_template(source, files):
    return filter(lambda f: f.endswith(".j2"), files)


def _render_systemd_templates(dest_path, ctx, src_dir):
    root_dir = f'{src_dir}/packaging/common/systemd/'
    jinja_ext = '.j2'
    files = ['redpanda.slice', 'redpanda.service', 'redpanda-tuner.service']
    for f in files:
        tmpl = f'{root_dir}{f}{jinja_ext}'
        templates.render_to_file(tmpl, os.path.join(dest_path, 'systemd', f),
                                 ctx)
    shutil.copy(os.path.join(root_dir, "50-redpanda.preset"),
                os.path.join(dest_path, "systemd"))
    shutil.copy(os.path.join(root_dir, "redpanda-status.service"),
                os.path.join(dest_path, "systemd"))
    shutil.copy(os.path.join(root_dir, "redpanda-status.timer"),
                os.path.join(dest_path, "systemd"))


def create_packages(vconfig, formats):
    global VERSION
    global REVISION
    global RELEASE

    REVISION = os.environ.get('SHORT_SHA', None)
    if not REVISION:
        repo = git.Repo(os.getcwd(), search_parent_directories=True)
        if repo:
            REVISION = repo.git.rev_parse(repo.head.object.hexsha, short=7)
        else:
            REVISION = '000000'

    tag = os.environ.get('TAG_NAME', None)
    if tag and 'release-' in tag:
        VERSION = tag.replace('release-', '')
    else:
        RELEASE = "dev"

    suffix = vconfig.product

    execs = [
        Exe(f'{vconfig.build_dir}/bin/{vconfig.product}'),
        Exe(f'{vconfig.go_out_dir}/rpk', dynamic=False),
        Exe(f'{vconfig.external_path}/bin/hwloc-calc', suffix),
        Exe(f'{vconfig.external_path}/bin/hwloc-distrib', suffix),
        Exe(f'{vconfig.external_path}/bin/iotune', suffix),
    ]

    dist_path = os.path.join(vconfig.build_dir, "dist")

    configs = [
        os.path.join(vconfig.src_dir, f"conf/{vconfig.product}.yaml"),
    ]
    admin_api_swag = glob.glob(
        os.path.join(vconfig.src_dir,
                     f"src/v/{vconfig.product}/admin/api-doc/*.json"))
    api_swag = glob.glob(
        os.path.join(vconfig.src_dir,
                     f"src/v/{vconfig.product}/api/api-doc/*.json"))

    os.makedirs(dist_path, exist_ok=True)

    formats = set(formats)

    if "dir" in formats:
        local_dir = os.path.join(dist_path, f"local/{vconfig.product}")
        _relocatable_dir(local_dir, execs, configs, admin_api_swag, api_swag,
                         vconfig)

    if formats.isdisjoint({"tar", "deb", "rpm"}):
        return

    tar_name = f'{vconfig.product}.tar.gz'
    tar_path = f"{dist_path}/{tar_name}"

    _relocable_tar_package(tar_path, execs, configs, admin_api_swag, api_swag,
                           vconfig)

    if 'tar' in formats:
        product_tar(vconfig.product, tar_path, dist_path)
    if 'deb' in formats:
        red_panda_deb(tar_path, dist_path, vconfig.src_dir, vconfig.environ)
    if 'rpm' in formats:
        red_panda_rpm(tar_path, dist_path, vconfig.src_dir, vconfig.environ)
    os.remove(tar_path)


def _patch_exe(root, exe, env):
    shell.run_subprocess(f'patchelf --set-interpreter {root}/lib/ld.so {exe}',
                         env=env)


def _render_thunk(root, bin_name):
    return f'''\
#!/usr/bin/env bash
set -e
export LD_LIBRARY_PATH="{root}/lib"
export PATH="{root}/bin:${{PATH}}"
exec -a "$0" "{root}/libexec/{bin_name}" "$@"
'''


class Exe:
    def __init__(self, path, suffix=None, dynamic=True):
        self.path = path
        self.suffix = suffix
        self.dynamic = dynamic

    def mangled_name(self):
        basename = os.path.basename(self.path)
        return f'{basename}-{self.suffix}' if self.suffix else basename
