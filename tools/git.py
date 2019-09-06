#!/usr/bin/env python3
import sys
import os
import logging
sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import shell
import log


def get_git_root(relative):
    return shell.run_oneline(
        "cd %s && git rev-parse --show-toplevel" % relative)


def get_git_user():
    return shell.run_oneline("git config user.name")


def get_tag_or_ref():
    head = shell.run_oneline("git rev-parse --short HEAD")
    tag = shell.run_oneline("git name-rev --tags --name-only %s" % head)
    return tag if tag != "undefined" else head


def get_git_files():
    ret = shell.raw_check_output("cd %s && git ls-files --full-name" %
                                 get_git_root(os.path.dirname(__file__)))
    return list(filter(lambda x: x and len(x) > 0, ret.split("\n")))


def get_git_changed_files(obj=None):
    if obj:
        ret = shell.raw_check_output(
            "cd {dir} && git show --name-only --format='' {obj}".format(
                dir=get_git_root(os.path.dirname(__file__)),
                obj=obj))
    else:
        ret = shell.raw_check_output(
            "cd %s && git diff --name-only --diff-filter=d" % get_git_root(
                os.path.dirname(__file__)))
    logger.debug("Files recently changed %s" % ret)
    return list(filter(lambda x: x and len(x) > 0, ret.split("\n")))


# tests
def main():
    import argparse

    def _generate_options():
        parser = argparse.ArgumentParser(description='build sys helper')
        parser.add_argument(
            '--log',
            type=str,
            default='INFO',
            help='info,debug, type log levels. i.e: --log=debug')
        parser.add_argument(
            '--files', type=str, default='all', help='changed | all')
        return parser

    parser = _generate_options()
    options, program_options = parser.parse_known_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s" % options)
    logger.info("Git root: %s", get_git_root(os.path.dirname(__file__)))
    if options.files == 'changed':
        for x in get_git_changed_files():
            print(x)
    if options.files == 'all':
        for x in get_git_files():
            print(x)


if __name__ == '__main__':
    main()
