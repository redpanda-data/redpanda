#!/usr/bin/env python3
import sys
import os
import logging
import re
sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import cpp
import golang
import shell
import git
import log


def _is_clang_fmt_file(filename):
    for ext in [".cc", ".cpp", ".h", ".hpp", ".proto", ".java", ".js"]:
        if filename.endswith(ext): return True
    return False


def _is_go_file(filename):
    return filename.endswith(".go")


def _is_clang_tidy_file(filename):
    for ext in [".cc", ".cpp"]:
        if filename.endswith(ext): return True
    return False


def _is_yapf_file(filename):
    return filename.endswith(".py")


def cpplint(files):
    logger.debug("Running cpplint")
    kLINT_OPTS = "--quiet --verbose=5 --counting=detailed"
    cpplint = cpp.get_cpplint()
    for f in files:
        if _is_clang_fmt_file(f):
            shell.run_subprocess("%s %s %s" % (cpplint, kLINT_OPTS, f))


def tidy(files):
    logger.debug("Running clang-tidy")
    kTIDY_OPTS = "-header-filter=.* -fix"
    clang_tidy = cpp.get_clang_tidy()
    for f in files:
        if _is_clang_tidy_file(f):
            shell.run_subprocess("%s %s %s" % (clang_tidy, kTIDY_OPTS, f))


def clangfmt(files):
    logger.debug("Running clang-format")
    clang_fmt = cpp.get_clang_format()
    for f in files:
        if _is_clang_fmt_file(f):
            shell.run_subprocess("%s -i %s" % (clang_fmt, f))


def crlfmt(files):
    logger.debug("Running crlfmt")
    crlfmt = golang.get_crlfmt()
    for f in files:
        if _is_go_file(f):
            shell.run_oneline("%s -w -diff=false -wrap=80 %s" % (crlfmt, f))


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(description='build sys helper')
        parser.add_argument(
            '--log',
            type=str,
            default='INFO',
            help='info,debug, type log levels. i.e: --log=debug')
        parser.add_argument(
            '--git-files',
            type=str,
            help='take file list from git object')
        return parser

    parser = generate_options()
    options, program_options = parser.parse_known_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s" % options)
    root = git.get_git_root(relative=os.path.dirname(__file__))

    def _files(r):
        return list(map(lambda x: "%s/%s" % (root, x), r))

    if options.git_files:
        r = git.get_git_changed_files(options.git_files)
    else:
        r = git.get_git_files()

    changed_files = _files(r)
    clangfmt(changed_files)


if __name__ == '__main__':
    main()
