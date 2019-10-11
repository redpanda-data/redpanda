#!/usr/bin/env python3
import sys
import os
import logging
import argparse
sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import shell
import log


def get_git_root(relative):
    return shell.run_oneline("cd %s && git rev-parse --show-toplevel" %
                             relative)


def get_git_user():
    return shell.run_oneline("git config user.name")


def get_git_email():
    return shell.run_oneline("git config user.email")


def get_git_sendemail_smtpuser():
    return shell.run_oneline("git config sendemail.smtpuser")


def get_git_sendemail_from():
    return shell.run_oneline('git config sendemail.from')


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
                dir=get_git_root(os.path.dirname(__file__)), obj=obj))
    else:
        ret = shell.raw_check_output(
            "cd %s && git diff --name-only --diff-filter=d" %
            get_git_root(os.path.dirname(__file__)))
    logger.debug("Files recently changed %s" % ret)
    return list(filter(lambda x: x and len(x) > 0, ret.split("\n")))


# tests
def main():
    parser = _build_parser()
    options = parser.parse_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s" % options)

    if options.files:
        _list_files(options.files)


def _build_parser():
    parser = argparse.ArgumentParser(description='build sys helper')
    parser.add_argument('--log',
                        type=str,
                        default='INFO',
                        help='info,debug, type log levels. i.e: --log=debug')
    parser.add_argument('--files',
                        type=str,
                        default='all',
                        help='changed | all')
    return parser


def _list_files(file_set):
    logger.info("Git root: %s", get_git_root(os.path.dirname(__file__)))
    if file_set == 'changed':
        for x in get_git_changed_files():
            print(x)
    if file_set == 'all':
        for x in get_git_files():
            print(x)


if __name__ == '__main__':
    main()
