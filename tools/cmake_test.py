#!/usr/bin/env python3
import sys
import os
import logging
import argparse
import tempfile
import random
import string
import shutil
from string import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import log
import shell
import fs


class TestRunner():
    def __init__(self, prepare_command, post_command, binary, repeat,
                 copy_files, *args):
        self.prepare_command = prepare_command
        self.post_command = post_command
        self.binary = binary
        self.repeat = repeat
        self.copy_files = copy_files
        self.root = "/dev/shm/vectorized_io"
        fs.mkdir_p(self.root)

        if "rpunit" in binary:
            self.test_args = " ".join([
                "--" if len(args) == 0 else " ".join(map(str, args)),
                "--overprovisioned", "--unsafe-bypass-fsync 1",
                "--blocked-reactor-notify-ms 2000000"
            ])
        elif "rpbench" in binary:
            # benchmarks have no boost wrapper which needs the silly `--` hack
            self.test_args = " ".join([
                " ".join(map(str, args)),
                "--blocked-reactor-notify-ms 2000000"
            ])
        else:
            self.test_args = "" if len(args) == 0 else " ".join(map(str, args))

    def _gen_alphanum(self, x=16):
        return ''.join(random.choice(string.ascii_letters) for _ in range(x))

    def _gen_testdir(self):
        return tempfile.mkdtemp(suffix=self._gen_alphanum(),
                                prefix="%s/test." % self.root)

    def run(self):
        test_dir = self._gen_testdir()
        env = os.environ.copy()
        env["TEST_DIR"] = test_dir
        env["BOOST_TEST_LOG_LEVEL"] = "test_suite"
        env["BOOST_TEST_COLOR_OUTPUT"] = "0"
        env["BOOST_TEST_CATCH_SYSTEM_ERRORS"] = "no"
        env["BOOST_TEST_REPORT_LEVEL"] = "no"
        env["BOOST_LOGGER"] = "HRF,test_suite"
        env["UBSAN_OPTIONS"] = "halt_on_error=1:abort_on_error=1"
        logger.info("Test dir: %s" % test_dir)
        for f in self.copy_files:
            logger.debug("Copying input file: %s" % f)
            (src, dst) = f.split("=") if "=" in f else (f, f)
            shutil.copy(src, "%s/%s" % (test_dir, os.path.basename(dst)))

        cmd = Template(
            "(cd $test_dir && $prepare_command && $binary $args && $post_command; e=$$?; "
            "rm -rf $test_dir; echo \"Test Exit code $$e\"; exit $$e)"
        ).substitute(test_dir=test_dir,
                     prepare_command=" && ".join(self.prepare_command)
                     or "true",
                     post_command=" && ".join(self.post_command) or "true",
                     binary=self.binary,
                     args=self.test_args)
        logger.info(cmd)
        os.execle("/bin/bash", "/bin/bash", "-c", cmd, env)


def main():
    def generate_options():
        parser = argparse.ArgumentParser(description='test helper for cmake')
        parser.add_argument('--binary', type=str, help='binary program to run')
        parser.add_argument('--pre',
                            nargs='*',
                            default=[],
                            type=str,
                            help='commands to run before test')
        parser.add_argument('--post',
                            nargs='*',
                            default=[],
                            type=str,
                            help='commands to run after test')
        parser.add_argument(
            '--log',
            type=str,
            default='DEBUG',
            help='info,debug, type log levels. i.e: --log=debug')
        parser.add_argument('--repeat',
                            type=int,
                            default=1,
                            help='how many times to repeat test')
        parser.add_argument('--copy_file',
                            type=str,
                            action="append",
                            help='copy file to test execution directory')
        return parser

    parser = generate_options()
    options, program_options = parser.parse_known_args()

    if not options.binary:
        parser.print_help()
        exit(1)
    if not options.copy_file:
        options.copy_file = []

    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s *args=%s" % (options, program_options))

    runner = TestRunner(options.pre, options.post, options.binary,
                        options.repeat, options.copy_file, *program_options)
    runner.run()
    return 0


if __name__ == '__main__':
    main()
