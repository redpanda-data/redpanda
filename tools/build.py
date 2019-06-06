#!/usr/bin/env python3

import os
import sys
import logging
import argparse

# add tools dir
sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import git
import shell
import log
import dependencies


def generate_options():
    parser = argparse.ArgumentParser(description='build sys helper')
    parser.add_argument(
        '--log',
        type=str.lower,
        default='info',
        choices=['critical', 'error', 'warning', 'info', 'debug'],
        help=
        'log level, one of ' \
        '[critical, error, warning, info, debug] i.e: --log debug'
    )
    parser.add_argument(
        '--deps',
        type=_str2bool,
        default='false',
        help='install 3rd party dependencies')
    parser.add_argument(
        '--build',
        type=str.lower,
        default='debug',
        choices=['debug', 'release', 'none'],
        help='choose of debug|release|none')
    parser.add_argument(
        '--targets',
        type=str.lower,
        nargs="*",
        default='all',
        choices=['all', 'cpp', 'go'],
        help='list of build targets [cpp, go]')
    parser.add_argument(
        '--files',
        type=str.lower,
        default='incremental',
        choices=['all', 'incremental'],
        help='files to format and to tidy: all | incremental')
    parser.add_argument(
        '--tidy',
        type=_str2bool,
        default='false',
        help='run formatter with clang-tidy')
    parser.add_argument(
        '--cpplint',
        type=_str2bool,
        default='true',
        help='run formatter with cpplint')
    parser.add_argument(
        '--fmt',
        type=_str2bool,
        default='true',
        help='format last changed files')
    parser.add_argument(
        '--clang',
        type=str,
        help='path to clang or `internal` keyword to use clang built internally' )
    parser.add_argument(
        '--packages',
        choices=['tar', 'rpm', 'deb'],  
        nargs='+',
        help='list of packages to create')
    return parser

def _str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def main():
    parser = generate_options()
    options, program_options = parser.parse_known_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s" % options)
    root = git.get_git_root(relative=os.path.dirname(__file__))
    if options.deps: dependencies.install_deps()
    
    # Importing packanges that requires dependencies installed by our script
    import build_helpers
    import fmt

    if options.build and options.build != "none":
        build_helpers.build(options.build, options.targets, options.clang)
    build_helpers.build_packages(options.build, options.packages)

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
