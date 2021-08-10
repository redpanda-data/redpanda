#!/usr/bin/env python3
# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
import os
import logging
import argparse
import tempfile
import random
import string
import shutil
import subprocess
from string import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')
fmt_string = '%(levelname)s:%(asctime)s %(filename)s:%(lineno)d] %(message)s'
logging.basicConfig(format=fmt_string)
formatter = logging.Formatter(fmt_string)
for h in logging.getLogger().handlers:
    h.setFormatter(formatter)

COMMON_TEST_ARGS = ["--blocked-reactor-notify-ms 2000000"]


class TestRunner():
    def __init__(self, prepare_command, post_command, binary, repeat,
                 copy_files, *args):
        self.prepare_command = prepare_command
        self.post_command = post_command
        self.binary = binary
        self.repeat = repeat
        self.copy_files = copy_files
        self.root = "/dev/shm/vectorized_io"
        os.makedirs(self.root, exist_ok=True)

        # make args a list
        if len(args) == 0: args = []
        else: args = list(map(str, args))

        if "rpunit" in binary:
            unit_args = [
                "--overprovisioned", "--unsafe-bypass-fsync 1",
                "--default-log-level=trace", "--logger-log-level='io=debug'",
                "--logger-log-level='exception=debug'"
            ] + COMMON_TEST_ARGS
            if "--" in args: args = args + unit_args
            else: args = args + ["--"] + unit_args
        elif "rpbench" in binary:
            args = args + COMMON_TEST_ARGS
        # aggregated args for test
        self.test_args = " ".join(args)

    def _gen_alphanum(self, x=16):
        return ''.join(random.choice(string.ascii_letters) for _ in range(x))

    def _gen_testdir(self):
        return tempfile.mkdtemp(suffix=self._gen_alphanum(),
                                prefix="%s/test." % self.root)

    def _addr2lines(self, backtrace):
        if not backtrace:
            # Maybe we saw a 'Backtrace:' line with no following
            # address lines, and got an empty list.  Ignore it.
            return

        # Find our build directory by searching back from binary
        path_parts = self.binary.split("/")
        try:
            vbuild = "/".join(path_parts[0:path_parts.index("vbuild") + 3])
        except (ValueError, IndexError):
            sys.stderr.write(
                f"Could not find vbuild in binary path {self.binary}")
            return
        else:
            addr2line_script = os.path.join(
                vbuild,
                "v_deps_build/seastar-prefix/src/seastar/scripts/seastar-addr2line"
            )

        ran = subprocess.run([addr2line_script, "-e", self.binary],
                             input="\n".join(backtrace),
                             encoding='utf-8',
                             capture_output=True)
        ran.check_returncode()

        sys.stderr.write("Captured a Seastar backtrace:\n")
        sys.stderr.write(ran.stderr)
        sys.stderr.write(ran.stdout)

    def _capture_backtraces(self, stderr):
        backtrace = None
        for line in stderr.split("\n"):
            if backtrace is None and line.startswith("Backtrace:"):
                backtrace = []
            elif backtrace is not None:
                if line.startswith("  "):
                    backtrace.append(line)
                else:
                    self._addr2lines(backtrace)
                    backtrace = None

        if backtrace:
            self._addr2lines(backtrace)

    def run(self):
        test_dir = self._gen_testdir()
        env = os.environ.copy()
        env["TEST_DIR"] = test_dir
        env["BOOST_TEST_LOG_LEVEL"] = "test_suite"
        if "CI" in env:
            env["BOOST_TEST_COLOR_OUTPUT"] = "0"
        env["BOOST_TEST_CATCH_SYSTEM_ERRORS"] = "no"
        env["BOOST_TEST_REPORT_LEVEL"] = "no"
        env["BOOST_LOGGER"] = "HRF,test_suite"
        env["UBSAN_OPTIONS"] = "halt_on_error=1:abort_on_error=1"
        env["ASAN_OPTIONS"] = "disable_coredump=0:abort_on_error=1"

        # FIXME: workaround for https://app.clubhouse.io/vectorized/story/897
        if "rpcgenerator_cycling_rpunit" in self.binary:
            env["UBSAN_OPTIONS"] = "halt_on_error=0:abort_on_error=0"

        logger.info("Test dir: %s" % test_dir)
        for f in self.copy_files:
            logger.debug("Copying input file: %s" % f)
            (src, dst) = f.split("=") if "=" in f else (f, f)
            shutil.copy(src, "%s/%s" % (test_dir, os.path.basename(dst)))

        cmd = Template(
            "(cd $test_dir; $prepare_command; $binary $args; $post_command e=$$?; "
            "rm -rf $test_dir; echo \"Test Exit code $$e\"; exit $$e)"
        ).substitute(test_dir=test_dir,
                     prepare_command=" && ".join(self.prepare_command)
                     or "true",
                     post_command=" && ".join(self.post_command) +
                     ";" if self.post_command else "",
                     binary=self.binary,
                     args=self.test_args)
        logger.info(cmd)
        # os.execle("/bin/bash", "/bin/bash", "-exc", cmd, env)

        # We only capture stderr becasuse that's where backtraces go
        ran = subprocess.run(cmd,
                             shell=True,
                             stderr=subprocess.PIPE,
                             encoding='utf-8')
        sys.stderr.write(ran.stderr)

        if ran.returncode != 0:
            # If there was at least one test failure, process the output
            # to extract Seastar backtraces, and run them through
            # seastar-addr2line to produce human readable output.
            self._capture_backtraces(ran.stderr)

        sys.exit(ran.returncode)


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

    logger.setLevel(getattr(logging, options.log.upper()))
    logger.info("%s *args=%s" % (options, program_options))

    runner = TestRunner(options.pre, options.post, options.binary,
                        options.repeat, options.copy_file, *program_options)
    runner.run()
    return 0


if __name__ == '__main__':
    main()
