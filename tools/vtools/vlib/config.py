import json
import os
import yaml

from absl import logging
from pathlib import Path

from . import kv


class VConfig(object):
    """Holds configuration options. Expected structure of .vtools.yml file:

    ```yaml
    build:
      root: <path> # path to build dir
      src: <path> # path to source folder
      default_type: [release | debug] # default build type
      gopath: <path> # GOPATH parent folder (e.g. /usr means GOPATH=/usr/go)
      clang: <path> # [optional] use clang if given (install if needed)
      external: <path> # [optional] external deps installed out of build dir
    ```
    """
    def __init__(self,
                 config_file=None,
                 product=None,
                 build_type=None,
                 clang=None):
        """Reads configuration and populates internal properties of the object
        based on contents of the file. If `config_file` is `None`, it
        recursively looks for a `.vtools.yml` file. The search is done from
        the current working directory up until a config file is found or the
        root of folder is reached. The configuration defaults to using GCC,
        unless `clang` is given. If the `build_type` arg is not given, the
        config looks for `build.default_type` in the YAML config, and throws an
        error if it's not defined.
        """
        # when config_file is given, working directory is os.getcwd()
        wd = os.getcwd()
        if not config_file:
            config_file = self.__find_config_file()
            if not config_file:
                logging.fatal('Unable to find .vtools.yml file.')
            # when config is found recursively, wd is config file's parent
            wd = os.path.dirname(config_file)

        logging.debug(f'Reading configuration file {config_file}.')
        with open(config_file, 'r') as f:
            self._cfg = yaml.safe_load(f)

        # make relative paths (w.r.t. working directory) absolute
        for k in self._cfg['build'].keys():
            if k == 'default_type':
                # the only option that is not a path
                continue
            if not os.path.isabs(self._cfg['build'][k]):
                self._cfg['build'][k] = f"{wd}/{self._cfg['build'][k]}"

        # check expected keys
        for k in ['gopath', 'root', 'src']:
            if not self._cfg['build'].get(k, None):
                logging.fatal(f"Expecting 'build.{k}' in {config_file}")

        if not build_type:
            build_type = self._cfg['build'].get('default_type', None)
            if not build_type:
                logging.fatal('Unable to determine build type.')
            logging.debug(f"Reading 'build_type' value from {config_file}")
        self._build_type = build_type
        logging.debug(f"Using '{self._build_type}' as build type.")

        if self.clang_path and not clang:
            # if YAML file defines path for clang, it implies we want clang
            clang = True
        elif clang and not self.clang_path:
            # use <build-root>/llvm/ if clang wanted but no clang_path given
            self.clang_path = f'{self.build_root}/llvm/llvm-bin'
        self._compiler = 'clang' if clang else 'gcc'

        if not self._cfg['build'].get('external', None):
            self._cfg['build']['external'] = (
                f'{self.build_dir}/v_deps_install')

        self._kv = kv.vectorized_kv(self._cfg['build']['src'])
        self._gopath = os.path.abspath(f"{self._cfg['build']['gopath']}/go")
        self._src_dir = os.path.abspath(self._cfg['build']['src'])
        self._node_build_dir = os.path.abspath(
            f"{self._cfg['build']['node_build_dir']}/node")

        # paths to dev utility binary folders (llvm, formatters, ansible, etc.)
        v_path = (f"{self._gopath}/bin:"
                  f"{self.build_root}/venv/v/bin:"
                  f"{self.build_root}/infra:"
                  f"{self.build_root}/infra/v2/current/bin:"
                  f"{self.java_home_dir}/bin:"
                  f"{self.maven_home_dir}/bin:"
                  f"{self.node_build_dir}/bin:"
                  "/bin:/usr/bin:/usr/local/bin:")
        if self.compiler == 'clang':
            v_path = f":{self.clang_path}/bin:{v_path}"

        # ansible
        self._ansible_tmp_dir = f'{self.build_root}/ansible'
        self._ansible_dir = f'{self.src_dir}/infra/ansible'

        # create a dict with minimal set of environment variables
        self._environ = {
            "PATH": v_path,
            "COMPILER": self.compiler,
            "BUILD_TYPE": self.build_type,
            "HOME": os.environ["HOME"],
            "GOPATH": self._gopath,
            "GOBIN": f'{self._gopath}/bin',
            'GOOS': 'linux',
            'GOARCH': 'amd64',
            "CGO_ENABLED": "0",
            "LC_CTYPE": "C.UTF-8",
            "CI": os.environ.get("CI", "0"),
            "JAVA_HOME": self.java_home_dir,
            "ANSIBLE_CONFIG": f'{self._ansible_dir}/ansible.cfg'
        }
        if self._environ["CI"] == "0":
            if "CCACHE_DIR" in os.environ:
                self._environ["CCACHE_DIR"] = os.environ.get("CCACHE_DIR")
                # check if /dev/shm/ exists
            if "CCACHE_DIR" not in os.environ and os.path.exists("/dev/shm"):
                self._environ["CCACHE_DIR"] = "/dev/shm/ccache"

        logging.debug(f"""Configuration:
  src_dir: {self.src_dir}
  build_dir: {self.build_dir}
  external_path: {self.external_path}
  compiler: {self.compiler}
  build_type: {self.build_type}
  go_path: {self.go_path}
  go_src_dir: {self.go_src_dir}
  go_out_dir: {self.go_src_dir}
  clang_path: {self.clang_path}
  node_build_dir: {self.node_build_dir}""")
        logging.debug(f'Environment: {json.dumps(self.environ, indent=4)}')

    @staticmethod
    def __find_config_file(curr_dir=os.getcwd()):
        """Attempts to recursively find a .vtools.yml file, starting from
        `os.getcwd()` up to the root of the file system. The search stops at
        the first match.
        """
        def search_vtools(thedir):
            if thedir == '/':
                return None

            for f in Path(thedir).glob('.vtools.yml'):
                logging.debug(f'Found {f}.')
                return f

            return search_vtools(os.path.dirname(thedir))

        return search_vtools(curr_dir)

    def go_out_dir(self, target_os=None, target_arch=None):
        """Output to binary output folder for go programs."""
        target_os = target_os or self.environ['GOOS']
        target_arch = target_arch or self.environ['GOARCH']
        return f"{self._cfg['build']['root']}/go/{target_os}/bin"

    @property
    def kv(self):
        return self._kv

    @property
    def src_dir(self):
        """Path to source directory folder."""
        return self._src_dir

    @property
    def build_root(self):
        """Path to root build folder."""
        return self._cfg['build']['root']

    @property
    def build_dir(self):
        """Path to build directory, which is the root plus build_type."""
        return f'{self.build_root}/{self.build_type}/{self.compiler}'

    @property
    def product(self):
        """Product."""
        return self._product

    @product.setter
    def product(self, value):
        self._product = value

    @property
    def build_type(self):
        """Build type."""
        return self._build_type

    @build_type.setter
    def build_type(self, value):
        self._build_type = value

    @property
    def compiler(self):
        return self._compiler

    @property
    def clang_path(self):
        """Path to clang folder (or None if no 'clang' defined)."""
        return self._cfg['build'].get('clang', None)

    @clang_path.setter
    def clang_path(self, value):
        self._cfg['build']['clang'] = value

    @property
    def external_path(self):
        """Path to install prefix for external projects."""
        return self._cfg['build']['external']

    @property
    def go_src_dir(self):
        """Source folder for go programs."""
        return f"{self._cfg['build']['src']}/src/go"

    @property
    def environ(self):
        """Sanitized environment variables."""
        return self._environ

    @property
    def go_path(self):
        """Path used for GOPATH variable."""
        return self._gopath

    @property
    def infra_bin_dir(self):
        """Path to infra binaries."""
        return os.path.join(self.build_root, 'infra')

    @property
    def java_home_dir(self):
        """Java home path"""
        return os.path.join(self.build_root, 'java')

    @property
    def maven_home_dir(self):
        """Maven home path"""
        return os.path.join(self.build_root, 'maven')

    @property
    def java_src_dir(self):
        """Source folder for java programs."""
        return f"{self._cfg['build']['src']}/src/java"

    @property
    def java_build_dir(self):
        """Build folder for java libraries"""
        return os.path.join(self.build_root, 'java-build')

    @property
    def java_bin_dir(self):
        """Folder for java binaries"""
        return os.path.join(self.build_root, 'bin')

    @property
    def ansible_dir(self):
        """Path to ansible dir containing config, roles, playbooks, etc."""
        return self._ansible_dir

    @property
    def ansible_tmp_dir(self):
        """Path to folder within build/ for logs and related ansible files."""
        return self._ansible_tmp_dir

    @property
    def node_build_dir(self):
        """Path to folder within build/."""
        return self._node_build_dir

    @property
    def node_src_dir(self):
        """Source folder for typescript/javascript programs."""
        return f"{self._cfg['build']['src']}/src/js"
