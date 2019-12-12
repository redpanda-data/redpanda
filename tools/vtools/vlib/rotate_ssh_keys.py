#!/usr/bin/env python3
import sys
import os
import logging
import time
import json
from datetime import datetime, timedelta
import random

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

# our types
import shell
import log
import fs
import git

KEY_TYPES = ["external", "internal", "deploy"]


def get_vectorized_keys_path():
    home = os.environ["HOME"]
    return "%s/.ssh/vectorized" % home


def next_vectorized_ssh_keys_folder():
    path = get_vectorized_keys_path()
    return "%s/%s" % (path, time.strftime("%Y.%m.%d"))


def get_latest_keys_dir():
    retval = []
    key_path = get_vectorized_keys_path()
    fs.mkdir_p(key_path)
    for root, dirs, files in os.walk(key_path, topdown=False):
        for d in dirs:
            if d != "current":
                retval.append("%s" % os.path.join(root, d))
    if len(retval) == 0: return None
    return sorted(retval, reverse=True)[0]


def is_ssh_key_path_timestamp_valid(path):
    if path == "": return False
    path_date = datetime.strptime(path.split("/")[-1], "%Y.%m.%d")
    expiry_date = path_date + timedelta(days=90)
    if expiry_date < datetime.now():
        logger.debug(
            f"Expired keys date {path_date} with expiration: {expiry_date}")
        return False
    logger.debug(f"Valid keys date {path_date} with expiration: {expiry_date}")
    return True


def get_key_comment():
    return "%s.%s" % (git.get_git_email(), time.strftime("%Y.%m.%d"))


def generate_keys():
    root = next_vectorized_ssh_keys_folder()
    fs.mkdir_p(root)
    for key_type in KEY_TYPES:
        comment = get_key_comment()
        output_file = "%s/%s_key" % (root, key_type)
        cmd = "ssh-keygen -t rsa -b 4096 -f %s -C %s" % (output_file, comment)
        if not os.path.exists(output_file): shell.run_subprocess(cmd)
        else: logger.info("File already exists: %s" % output_file)
    return root


def _fprint():
    root = get_latest_keys_dir()
    retval = {}
    for key_type in KEY_TYPES:
        output_file = "%s/%s_key" % (root, key_type)
        logger.debug(f"fingerprint {output_file}")
        fprint_cmd = "ssh-keygen -l -E md5 -f %s.pub" % output_file
        fprint = shell.run_oneline(fprint_cmd)
        retval[output_file] = fprint
    # return a map of the keys
    return retval


def _match_filesystem(fingerprints):
    root = get_latest_keys_dir()
    fingerprint_file = "%s/fingerprint" % root
    if not os.path.exists(fingerprint_file): return False
    with open(fingerprint_file, 'r') as content_file:
        old_fprints = json.loads(content_file)
        for k, v in old_fprints:
            if k not in fingerprints: return False
    return True


# no validation
# Should only be called after generate keys
def fingerprint_keys():
    root = get_latest_keys_dir()
    fingerprint_file = "%s/fingerprint" % root
    fprints = _fprint()
    if os.path.exists(fingerprint_file):
        if not _match_filesystem(fprints):
            os.remove(fingerprint_file)
        else:
            logger.info("Matching fingerprints")
            logger.info(fprints)
            return
    with open(fingerprint_file, 'w') as f:
        f.write(json.dumps(fprints, indent=4, sort_keys=True))


def symlink_new_keys(latest_dir):
    current = "%s/current" % get_vectorized_keys_path()
    fs.mkdir_p(current)
    logger.info("Executing in directory: %s" % current)
    os.chdir(current)
    all_symlink_keys = ["fingerprint"] + list(
        map(lambda x: "%s_key" % x, KEY_TYPES)) + list(
            map(lambda x: "%s_key.pub" % x, KEY_TYPES))

    for f in all_symlink_keys:
        target = "%s/%s" % (latest_dir, f)
        relative_file = os.path.relpath(target, f)[3:]
        fs.force_symlink(relative_file, f)


def needs_rotation():
    keys_dir = get_latest_keys_dir()
    if keys_dir is None:
        logger.debug("no vectorized ssh key discovered: %s" %
                     get_vectorized_keys_path())
        return True
    if is_ssh_key_path_timestamp_valid(keys_dir):
        return False

    root = next_vectorized_ssh_keys_folder()
    for key_type in KEY_TYPES:
        output_file = "%s/%s_key" % (root, key_type)
        if not os.path.exists(output_file):
            logger.debug("key %s does not exist" % output_file)
            return True

    return False


def main():
    import argparse

    def _generate_options():
        parser = argparse.ArgumentParser(description='ssh keys manager')
        parser.add_argument(
            '--log',
            type=str,
            default='INFO',
            help='info,debug, type log levels. i.e: --log=debug')
        return parser

    parser = _generate_options()
    options, program_options = parser.parse_known_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info(f"{options}")

    if not needs_rotation():
        logger.info("all good!")
        logger.info(json.dumps(_fprint(), indent=4, sort_keys=True))
        return 0

    latest_dir = generate_keys()
    # make sure fingerprints match
    fingerprint_keys()
    # always check the symlinks
    symlink_new_keys(latest_dir)
    logger.info("Remember:")
    logger.info("1. Use your external key for github & external accounts")
    logger.info("2. Use your internal key for VPN systems")
    logger.info("3. Use your deploy key for our production systems")


if __name__ == '__main__':
    main()
