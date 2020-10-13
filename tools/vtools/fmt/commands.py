import click
import difflib
import subprocess

from absl import logging
from ..vlib import config
from ..vlib import shell


@click.group(short_help='format cpp, go, python and shell code')
def fmt():
    pass


@fmt.command(short_help='runs crlfmt against go source code files.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory.'),
              default=None)
@click.option('--check',
              help=('Do not format in-place; instead, check whether files are '
                    'properly formatted and throw an error if they are not.'),
              is_flag=True)
@click.option('--ref',
              help=('Obtain list of files to process by comparing the current '
                    'state of the repository and compare it against the given '
                    'gitref.'),
              default=None)
def go(conf, ref, check):
    vconfig = config.VConfig(conf)
    _crlfmt(vconfig, ref, check)


@fmt.command(short_help='runs shfmt against shell scripts.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--check',
              help=('Do not format in-place; instead, check whether files are '
                    'properly formatted and throw an error if they are not'),
              is_flag=True)
@click.option('--ref',
              help=('Obtain list of files to process by comparing the current '
                    'state of the repository and compare it against the given '
                    'gitref.'),
              default=None)
def sh(conf, ref, check):
    vconfig = config.VConfig(conf)
    _shfmt(vconfig, ref, check)


@fmt.command(short_help='runs terraform fmt against Terraform modules.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--check',
              help=('Do not format in-place; instead, check whether files are '
                    'properly formatted and throw an error if they are not'),
              is_flag=True)
@click.option('--ref',
              help=('Obtain list of files to process by comparing the current '
                    'state of the repository and compare it against the given '
                    'gitref.'),
              default=None)
def tf(conf, ref, check):
    vconfig = config.VConfig(conf)
    _tffmt(vconfig, ref, check)


@fmt.command(short_help='runs clang-format against C++ source code.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--check',
              help=('Do not format in-place; instead, check whether files are '
                    'properly formatted and throw an error if they are not'),
              is_flag=True)
@click.option('--ref',
              help=('Obtain list of files to process by comparing the current '
                    'state of the repository and compare it against the given '
                    'gitref.'),
              default=None)
def cpp(conf, ref, check):
    vconfig = config.VConfig(conf, clang=True)
    _clangfmt(vconfig, ref, check)


@fmt.command(short_help='runs yapf for python source files.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--check',
              help=('Do not format in-place; instead, check whether files are '
                    'properly formatted and throw an error if they are not'),
              is_flag=True)
@click.option('--ref',
              help=('Obtain list of files to process by comparing the current '
                    'state of the repository and compare it against the given '
                    'gitref.'),
              default=None)
def py(conf, ref, check):
    vconfig = config.VConfig(conf)
    _yapf(vconfig, ref, check)


@fmt.command(short_help='runs npm fmt on v/js')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--check',
              help=('Do not format in-place; instead, check whether files are '
                    'properly formatted and throw an error if they are not'),
              is_flag=True)
@click.option('--ref',
              help=('Obtain list of files to process by comparing the current '
                    'state of the repository and compare it against the given '
                    'gitref.'),
              default=None)
def js(conf, ref, check):
    vconfig = config.VConfig(conf)
    _jsfmt(vconfig, ref, check)


@fmt.command(short_help='shortcut for applying all formatters.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--check',
              help=('Do not format in-place; instead, check whether files are '
                    'properly formatted and throw an error if they are not'),
              is_flag=True)
@click.option('--ref',
              help=('Obtain list of files to process by comparing the current '
                    'state of the repository and compare it against the given '
                    'gitref.'),
              default=None)
def all(conf, ref, check):
    all_changed(conf, ref=ref, check=check)


def all_changed(conf, ref=None, check=True):
    vconfig = config.VConfig(conf, clang=True)
    _clangfmt(vconfig, ref, check)
    _crlfmt(vconfig, ref, check)
    _yapf(vconfig, ref, check)
    _shfmt(vconfig, ref, check)
    _tffmt(vconfig, ref, check)
    _jsfmt(vconfig, ref, check)


@fmt.command(short_help='runs clang-tidy against redpanda for clang builds.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--check',
              help=('Do not format in-place; instead, check whether files are '
                    'properly formatted and throw an error if they are not'),
              is_flag=True)
def tidy(conf, check):
    vconfig = config.VConfig(conf, clang=True)
    args = f'-p compile_commands.json {vconfig.src_dir}/src/v/redpanda/main.cc'
    args = f'{args} {"" if check else "--fix"}'
    shell.raw_check_output(f'cd {vconfig.build_dir} && clang-tidy {args}',
                           env=vconfig.environ)


def _clangfmt(vconfig, ref, check):
    logging.debug("Running clang-format")
    args = f'-style=file -fallback-style=none {"" if check else "-i"}'
    exts = [".cc", ".cpp", ".h", ".hpp", ".proto", ".java"]
    _fmt(vconfig, exts, 'clang-format', args, ref, check)


def _crlfmt(vconfig, ref, check):
    logging.debug("Running crlfmt")
    args = f'-wrap=80 {"" if check else "-diff=false -w"}'
    _fmt(vconfig, ['.go'], 'crlfmt', args, ref, check)


def _jsfmt(vconfig, ref, check):
    logging.debug("Running Prettier")
    args = f'--list-different' if check else f'--write'
    _fmt(vconfig, [".ts", ".js"], 'npx prettier', args, ref, check)


def _yapf(vconfig, ref, check):
    logging.debug("Running yapf")
    args = f'{"-d" if check else "-i"}'
    _fmt(vconfig, ['.py'], 'yapf', args, ref, check)


def _shfmt(vconfig, ref, check):
    logging.debug("Running shfmt")
    args = f'-i 2 -ci -s {"-d" if check else "-w"}'
    _fmt(vconfig, ['.sh'], 'shfmt', args, ref, check)


def _tffmt(vconfig, ref, check):
    logging.debug("Running terraform fmt")
    args = f'-recursive {"-check -diff" if check else ""}'
    _fmt(vconfig, [".tf"], "terraform fmt", args, ref, check)


def _fmt(vconfig, exts, cmd, args, ref, check):
    for f in _git_files(vconfig, exts, ref):
        f = f'{vconfig.src_dir}/{f}'
        try:
            ret = shell.raw_check_output(
                f'cd {vconfig.src_dir} && {cmd} {args} {f}',
                env=vconfig.environ)
        except subprocess.CalledProcessError as e:
            # some formatters return non-zero if they find differences. So we
            # print whatever they have to tell us and fail
            logging.fatal(e.output.decode('utf-8'))

        if not check:
            # move to next file if no check required
            continue

        if 'clang-format' in cmd:
            # clang-format doesn't support -diff, so we need to do it ourselves
            #
            # we could alternatively use git and change in-place, followed by
            # a 'git diff' but that'd assume there are no unstaged changes,
            # which might not be the case in a scenario where a developer wants
            # to do 'vtools fmt all --check' just to see if there's anything
            # wrong, without updating in-place
            with open(f'{f}', 'r') as checked_file:
                before = checked_file.read()
                after = ret
                ret = None
                if before != after:
                    diff = difflib.unified_diff(before,
                                                after,
                                                lineterm='\n',
                                                fromfile=f'a/{f}',
                                                tofile=f'b/{f}')
                    ret = f'\n{"".join(diff)}'

        if ret:
            logging.fatal(ret)


def _git_files(vconfig, exts, ref):
    if ref:
        cmd = f'git -C {vconfig.src_dir} diff --diff-filter=AM --name-only {ref}'
    else:
        cmd = f'git -C {vconfig.src_dir} ls-files --full-name'
    ret = shell.raw_check_output(cmd, env=vconfig.environ)

    # FIXME: remove once clang-format bug is solved (treated as objective-C)
    objective_c_not = ['src/v/kafka/errors.h']
    for f in ret.split("\n"):
        for e in exts:
            if f.endswith(e) and f not in objective_c_not:
                yield f
