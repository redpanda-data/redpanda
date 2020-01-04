import os
import yaml

from absl import logging
from pathlib import Path


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
    def __init__(self, config_file=None, top_folder=None, build_type=None,
                 clang=None):
        """Reads configuration and populates internal properties of the object
        based on contents of the file. If `config_file` is `None`, it
        recursively looks for a `.vtools.yml` file. The search is done from
        the folder pointed to by `top_folder` or current working directory if
        `top_folder` is `None`. The configuration defaults to using GCC, unless
        `clang` is given. If the `build_type` arg is not given, the config
        looks for `build.default_type` in the YAML config, and throws an error
        if it's not defined.
        """
        if not config_file:
            config_file = self.__find_config_file(top_folder)

        logging.info(f'Reading configuration file {config_file}.')
        with open(config_file, 'r') as f:
            self._cfg = yaml.safe_load(f)

        # make paths absolute
        for k in self._cfg['build'].keys():
            if k == 'default_type':
                # the only option that is not a path
                continue
            self._cfg['build'][k] = os.path.abspath(self._cfg['build'][k])

        # check expected keys
        for k in ['gopath', 'root', 'src']:
            if not self._cfg['build'].get(k, None):
                logging.fatal(f"Expecting 'build.{k}' in {config_file}")

        if not build_type:
            build_type = self._cfg['build'].get('default_type', None)
            if not build_type:
                logging.fatal('Unable to determine build type.')
            logging.info(f"Using 'build_type' value from {config_file}")
        self._build_type = build_type
        logging.info(f"Using '{self._build_type}' as build type.")

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

        # Set Go-specific environment variables (GOPATH and PATH). This
        # modifies environment for the current process and its children.
        self._gopath = f"{self._cfg['build']['gopath']}/go"
        os.environ['GOPATH'] = self._gopath
        os.environ['PATH'] = f"{self._gopath}/bin:{os.environ['PATH']}"

        logging.debug(f"""Configuration:
  src_dir: {self.src_dir}
  build_dir: {self.build_dir}
  external_path: {self.external_path}
  go_path: {self.go_path}
  go_src_dir: {self.go_src_dir}
  go_out_dir: {self.go_src_dir}
  clang_path: {self.clang_path}""")
        logging.debug(f'Environment: {os.environ}')

    @staticmethod
    def __find_config_file(top_folder=None):
        """Attempts to recursively find a .vtools.yml file, starting from
        `top_folder`. If `top_folder` is `None`, the search starts at
        `os.getcwd()`. The first match is returned.
        """
        if not top_folder:
            top_folder = os.getcwd()

        for f in Path(top_folder).rglob('.vtools.yml'):
            logging.info(f'Found {f}.')
            return f

        logging.fatal('Unable to find .vtools.yml file.')

    @property
    def src_dir(self):
        """Path to source directory folder."""
        return self._cfg['build']['src']

    @property
    def build_root(self):
        """Path to root build folder."""
        return self._cfg['build']['root']

    @property
    def build_dir(self):
        """Path to build directory, which is the root plus build_type."""
        return f'{self.build_root}/{self.build_type}/{self.compiler}'

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
    def go_out_dir(self):
        """Output to binary output folder for go programs."""
        return f"{self._cfg['build']['root']}/go/bin"

    @property
    def go_path(self):
        """Path used for GOPATH variable."""
        return self._gopath
