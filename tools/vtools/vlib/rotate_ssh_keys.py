import os
import time
import json
from datetime import datetime, timedelta
from git import Repo
from absl import logging
from . import shell
from . import fs

KEY_TYPES = ["external", "internal", "deploy"]


def rotate_ssh_keys(env=os.environ):
    latest_dir = generate_keys(env)
    # make sure fingerprints match
    fingerprint_keys()
    # always check the symlinks
    symlink_new_keys(latest_dir)


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
        logging.debug(
            f"Expired keys date {path_date} with expiration: {expiry_date}")
        return False
    logging.debug(
        f"Valid keys date {path_date} with expiration: {expiry_date}")
    return True


def get_key_comment():
    r = Repo('.', search_parent_directories=True)
    reader = r.config_reader()
    email = reader.get_value("user", "email")
    return "%s.%s" % (email, time.strftime("%Y.%m.%d"))


def generate_keys(env=os.environ):
    root = next_vectorized_ssh_keys_folder()
    fs.mkdir_p(root)
    for key_type in KEY_TYPES:
        comment = get_key_comment()
        output_file = "%s/%s_key" % (root, key_type)
        generate_key(output_file, comment, env=env)
    return root


def generate_key(path, comment, password=None, env=os.environ):
    cmd = f'ssh-keygen -t rsa -b 4096 -f {path} -C {comment}'
    pub_path = f'{path}.pub'
    if password is not None:
        cmd = f'{cmd} -P {password}'
    if not os.path.exists(path):
        shell.run_subprocess(cmd, env=env)
    else:
        logging.info(f'File already exists: {path}')
    return path, pub_path


def _fprint(env):
    root = get_latest_keys_dir()
    retval = {}
    for key_type in KEY_TYPES:
        output_file = "%s/%s_key" % (root, key_type)
        logging.debug(f"fingerprint {output_file}")
        fprint_cmd = "ssh-keygen -l -E md5 -f %s.pub" % output_file
        fprint = shell.run_oneline(fprint_cmd, env=env)
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
def fingerprint_keys(env):
    root = get_latest_keys_dir()
    fingerprint_file = "%s/fingerprint" % root
    fprints = _fprint(env)
    if os.path.exists(fingerprint_file):
        if not _match_filesystem(fprints):
            os.remove(fingerprint_file)
        else:
            logging.info("Matching fingerprints")
            logging.info(fprints)
            return
    with open(fingerprint_file, 'w') as f:
        f.write(json.dumps(fprints, indent=4, sort_keys=True))


def symlink_new_keys(latest_dir):
    current = "%s/current" % get_vectorized_keys_path()
    fs.mkdir_p(current)
    logging.info("Executing in directory: %s" % current)
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
        logging.debug("no vectorized ssh key discovered: %s" %
                      get_vectorized_keys_path())
        return True
    if is_ssh_key_path_timestamp_valid(keys_dir):
        return False

    root = next_vectorized_ssh_keys_folder()
    for key_type in KEY_TYPES:
        output_file = "%s/%s_key" % (root, key_type)
        if not os.path.exists(output_file):
            logging.debug("key %s does not exist" % output_file)
            return True

    return False
