#!/usr/bin/env python3
import sys
import os
import logging
import argparse
import re
import subprocess
sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import cli
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


def main():
    parser = _build_parser()
    options = parser.parse_args()

    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s" % options)

    if options.check_config:
        _check_git_config()
    elif options.files:
        _list_files(options.files)


def _build_parser():
    parser = argparse.ArgumentParser(description='Git helper')
    parser.add_argument('--log',
                        type=str,
                        default='INFO',
                        help='info, debug, type log levels. i.e: --log=debug')
    parser.add_argument('--check-config',
                        type=cli.str2bool,
                        default='false',
                        help='Check your git config')
    parser.add_argument('--files',
                        type=str,
                        default='all',
                        help='changed | all. List files')
    return parser


def _list_files(file_set):
    logger.info("Git root: %s", get_git_root(os.path.dirname(__file__)))
    if file_set == 'changed':
        for x in get_git_changed_files():
            print(x)
    if file_set == 'all':
        for x in get_git_files():
            print(x)


def _check_git_config():
    validation_rules = [
        UserNameValidationRule(),
        UserEmailValidationRule(),
        SmtpUserValidationRule(),
        FromValidationRule(),
    ]

    invalid_rules = [r for r in validation_rules if not r.checked_is_valid()]

    for rule in invalid_rules:
        logger.error(rule.msg())
        if rule.exception:
            logger.debug(rule.exception)

    if len(invalid_rules) > 0:
        logger.error('Please fix git config settings')
        sys.exit(1)

    logger.info('Git config successfully verified')


class ValidationRule():
    def msg(self):
        raise NotImplementedError()

    def _is_valid(self):
        raise NotImplementedError()

    def checked_is_valid(self):
        try:
            return self._is_valid()
        except subprocess.CalledProcessError as e:
            self.exception = e
            return False

    def err_msg(self):
        if self.exception:
            return 'Command ' + self.exception.cmd + ' failed with return code ' + str(
                self.exception.returncode)


class UserNameValidationRule(ValidationRule):
    def msg(self):
        return "Please set user name with 'git config user.name <user_name>'"

    def _is_valid(self):
        return get_git_user() and get_git_user().strip()


class UserEmailValidationRule(ValidationRule):
    def msg(self):
        return "Please set user email with 'git config user.email <user>@vectorized.io'"

    def _is_valid(self):
        return get_git_email().endswith('@vectorized.io')


class SmtpUserValidationRule(ValidationRule):
    def msg(self):
        return "Please set the smtp user with 'git config sendemail.smtpuser <user>@vectorized.io'"

    def _is_valid(self):
        return get_git_sendemail_smtpuser().endswith('@vectorized.io')


class FromValidationRule(ValidationRule):
    def msg(self):
        return "Please set the from field with `git config sendemail.from '\"name\" <email>'`"

    def _is_valid(self):
        email_pattern = r'\"[0-9a-zA-Z\s]+\"\<[0-9a-zA-Z]+\@[0-9a-zA-Z]+\.[0-9a-zA-Z]+\>'
        return re.match(email_pattern, get_git_sendemail_from())


if __name__ == '__main__':
    main()
