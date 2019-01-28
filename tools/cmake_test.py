#!/usr/bin/env python3
import sys
import os
import logging
import argparse
import tempfile
import random
import string
import shutil

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import log
import shell


class TestRunner():
    def __init__(self, binary, repeat, base_directory, copy_files, *args):
        self.binary = binary
        self.repeat = repeat
        self.base_directory = base_directory
        self.copy_files = copy_files
        self.test_args = "" if len(args) == 0 else " ".join(map(str, args))

    def _gen_alphanum(self, x=16):
        return ''.join(
            random.choice(string.ascii_letters + string.digits)
            for _ in range(x))

    def _gen_testdir(self):
        return tempfile.mkdtemp(
            suffix=self._gen_alphanum(),
            prefix="%s.test." % self.base_directory)

    def _env(self):
        e = os.environ.copy()
        e["GLOG_logtostderr"] = '1'
        e["GLOG_v"] = '1'
        e["GLOG_vmodule"] = ''
        e["GLOG_logbufsecs"] = '0'
        e["GLOG_log_dir"] = '.'
        e["GTEST_COLOR"] = 'no'
        e["GTEST_SHUFFLE"] = '1'
        e["GTEST_BREAK_ON_FAILURE"] = '1'
        e["GTEST_REPEAT"] = '1'
        e["TEST_DIR"] = self._gen_testdir()
        return e

    def run(self):
        env = {}
        try:
            env = self._env()
            logger.info("Test dir: %s" % env["TEST_DIR"])
            for f in self.copy_files:
                logger.debug("Copying input file: %s" % f)
                (src, dst) = f.split("=") if "=" in f else (f,f)
                shutil.copy(src, "%s/%s" % (env["TEST_DIR"],
                                            os.path.basename(dst)))
            for i in range(self.repeat):
                cmd = "cd %s && %s %s" % (env["TEST_DIR"], self.binary,
                                          self.test_args)
                logger.info("Executing test iteration: %s" % i)
                shell.run_subprocess(cmd, env)

        except Exception as e:
            logger.exception(e)
            raise e
        if "TEST_DIR" in env:
            logger.info("Removing: %s" % env["TEST_DIR"])
            shutil.rmtree(env["TEST_DIR"])


def main():
    def generate_options():
        parser = argparse.ArgumentParser(description='test helper for cmake')
        parser.add_argument('--binary', type=str, help='binary program to run')
        parser.add_argument(
            '--log',
            type=str,
            default='DEBUG',
            help='info,debug, type log levels. i.e: --log=debug')
        parser.add_argument(
            '--base_directory',
            type=str,
            help='usually ${CMAKE_CURRENT_BINARY_DIR}')
        parser.add_argument(
            '--repeat',
            type=int,
            default=1,
            help='how many times to repeat test')
        parser.add_argument(
            '--copy_file',
            type=str,
            action="append",
            help='copy file to test execution directory')
        return parser

    parser = generate_options()
    options, program_options = parser.parse_known_args()

    if not options.binary:
        parser.print_help()
        exit(1)
    if not options.base_directory:
        parser.print_help()
        exit(1)
    if not options.copy_file:
        options.copy_file = []

    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s *args=%s" % (options, program_options))

    runner = TestRunner(options.binary, options.repeat, options.base_directory,
                        options.copy_file, *program_options)
    runner.run()
    return 0


if __name__ == '__main__':
    main()
