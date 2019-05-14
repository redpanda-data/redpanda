#!/usr/bin/env python3

import os
import sys
import logging
import argparse
import distutils.util

# add tools dir
sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import git
import shell
import fmt
import log
import packaging
from build_helpers import *


def generate_options():
    parser = argparse.ArgumentParser(description='build sys helper')
    parser.add_argument(
        '--log',
        type=str,
        default='INFO',
        help='info,debug, type log levels. i.e: --log=debug')
    parser.add_argument(
        '--deps',
        type=distutils.util.strtobool,
        default='false',
        help='install 3rd party dependencies')
    parser.add_argument(
        '--build',
        type=str,
        default='debug',
        help='choose of debug|release|none')
    parser.add_argument(
        '--targets',
        nargs="*",
        default='all',
        help='list of build targets [cpp, go]')
    parser.add_argument(
        '--files',
        type=str,
        default='incremental',
        help='files to format and to tidy: all | incremental')
    parser.add_argument(
        '--tidy',
        type=distutils.util.strtobool,
        default='false',
        help='run formatter with clang-tidy')
    parser.add_argument(
        '--cpplint',
        type=distutils.util.strtobool,
        default='true',
        help='run formatter with cpplint')
    parser.add_argument(
        '--fmt',
        type=distutils.util.strtobool,
        default='true',
        help='format last changed files')
    parser.add_argument(
        '--clang',
        type=str,
        help='path to clang or `internal` keyword to use clang built internally' )
    parser.add_argument(
        '--packages',
        choices=['tar'],  
        nargs='+',
        help='list of packages to create')
    return parser


def main():
    parser = generate_options()
    options, program_options = parser.parse_known_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s" % options)
    root = git.get_git_root(relative=os.path.dirname(__file__))
    if options.deps: install_deps()
    if options.build and options.build != "none":
        build(options.build, options.targets, options.clang)
    build_packages(options.build, options.packages)

    def _files():
        r = []
        if options.files == "incremental":
            r = git.get_git_changed_files()
        if options.files == "all":
            r = git.get_git_files()
        return list(map(lambda x: "%s/%s" % (root, x), r))

    changed_files = _files()
    if options.fmt:
        fmt.clangfmt(changed_files)
        fmt.crlfmt(changed_files)
    if options.tidy:
        fmt.tidy(changed_files)
    if options.cpplint:
        fmt.cpplint(changed_files)


if __name__ == '__main__':
    main()
