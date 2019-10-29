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
    def __init__(self, binary, repeat, copy_files, *args):
        self.binary = binary
        self.repeat = repeat
        self.copy_files = copy_files
        self.test_args = "" if len(args) == 0 else " ".join(map(str, args))
        self.root = "/dev/shm/vectorized_io"
        # our ci does not have shared mem on the dockerfile mounted
        if "CI" in os.environ:
            self.root = "/tmp/vectorized_io"
        fs.mkdir_p(self.root)

    def _gen_alphanum(self, x=16):
        return ''.join(random.choice(string.ascii_letters) for _ in range(x))

    def _gen_testdir(self):
        return tempfile.mkdtemp(suffix=self._gen_alphanum(),
                                prefix="%s/test." % self.root)

    def run(self):
        test_dir = self._gen_testdir()
        env = os.environ.copy()
        env["TEST_DIR"] = test_dir
        logger.info("Test dir: %s" % test_dir)
        for f in self.copy_files:
            logger.debug("Copying input file: %s" % f)
            (src, dst) = f.split("=") if "=" in f else (f, f)
            shutil.copy(src, "%s/%s" % (test_dir, os.path.basename(dst)))

        cmd = Template(
            "(cd $test_dir && $binary $args; e=$$?; "
            "rm -rf $test_dir; echo \"Test Exit code $$e\"; exit $$e)").substitute(
                test_dir=test_dir, binary=self.binary, args=self.test_args)
        logger.info(cmd)
        os.execle("/bin/bash", "/bin/bash", "-c", cmd, env)


def main():
    def generate_options():
        parser = argparse.ArgumentParser(description='test helper for cmake')
        parser.add_argument('--binary', type=str, help='binary program to run')
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

    runner = TestRunner(options.binary, options.repeat, options.copy_file,
                        *program_options)
    runner.run()
    return 0


if __name__ == '__main__':
    main()
