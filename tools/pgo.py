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
import log


def generate_options():
    parser = argparse.ArgumentParser(description='PGO program helper')
    parser.add_argument(
        '--log',
        type=str,
        default='INFO',
        help='info,debug, type log levels. i.e: --log=debug')
    parser.add_argument(
        '--binary', type=str, default='redpanda', help='binary to PGO')
    parser.add_argument('--type', type=str, help='either generate or use')

    return parser


def _pgo_gen(options):
    root = git.get_git_root(relative=os.path.dirname(__file__))
    build_root = "%s/build" % root
    pgo_build = "%s/%s" % (build_root, "pgo_generate")
    if not os.path.exists(pgo_build): os.makedirs(pgo_build)
    cmake_cmd = ("cmake -G Ninja"
                 " -DCMAKE_BUILD_TYPE=Release "
                 " -DRP_PGO=-fprofile-generate=%s") % pgo_build
    shell.run_subprocess("cd %s && %s %s " % (pgo_build, cmake_cmd, root))
    shell.run_subprocess("cd %s && ninja %s " % (pgo_build, options.binary))


def _pgo_use(options):
    root = git.get_git_root(relative=os.path.dirname(__file__))
    build_root = "%s/build" % root
    pgo_build = "%s/%s" % (build_root, "pgo_use")
    pgo_gen_build = "%s/%s" % (build_root, "pgo_generate")
    if not os.path.exists(pgo_build): os.makedirs(pgo_build)
    # We need -fprofile-correction due to a bug in the profiler code w/ gcc
    #
    cmake_cmd = (
        "cmake -G Ninja"
        " -DCMAKE_BUILD_TYPE=Release "
        " -DRP_PGO=\"-fprofile-use=%s -fprofile-correction\"") % pgo_gen_build
    shell.run_subprocess("cd %s && %s %s " % (pgo_build, cmake_cmd, root))
    shell.run_subprocess("cd %s && ninja %s " % (pgo_build, options.binary))


def main():
    parser = generate_options()
    options, program_options = parser.parse_known_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s" % options)
    if options.type == "generate": _pgo_gen(options)
    elif options.type == "use": _pgo_use(options)
    else: logger.error("Please pass in --type=generate or --type=use for pgo")


if __name__ == '__main__':
    main()
