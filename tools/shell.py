#!/usr/bin/env python3
import subprocess
import sys
import re
import os
import logging
sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')


def run_subprocess(cmd, env=os.environ):
    logger.debug("Running command: exec bash -c '%s'" % cmd)
    proc = subprocess.Popen(
        "exec bash -c '%s'" % cmd,
        stdout=sys.stdout,
        stderr=sys.stderr,
        env=env,
        shell=True)
    return_code = 0
    try:
        return_code = proc.wait()
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception as e:
        proc.kill()
        raise
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, cmd)


def _cleanup_whitespace(s):
    # removes leading and trailing spaces
    # removes duplicate spaces, i.e.: `foo    bar` == `foo bar`
    return re.sub(r' +', ' ', s.strip())


def raw_check_output(cmd):
    logger.debug("raw_check_output: %s", cmd)
    ret = subprocess.check_output(cmd, shell=True)
    if ret is None: return ret
    return str(ret.decode("utf-8"))


def run_oneline(cmd):
    logger.debug("run_oneline: %s", cmd)
    ret = raw_check_output(cmd)
    if ret is None: return ret
    return _cleanup_whitespace(ret)
