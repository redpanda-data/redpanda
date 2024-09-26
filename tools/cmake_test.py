#!/usr/bin/env python3
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import signal
import sys
import os
import logging
import argparse
import tempfile
import random
import string
import shutil
import subprocess
import threading
import re
from string import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')
fmt_string = '%(levelname)s:%(asctime)s %(filename)s:%(lineno)d] %(message)s'
logging.basicConfig(format=fmt_string)
formatter = logging.Formatter(fmt_string)
for h in logging.getLogger().handlers:
    h.setFormatter(formatter)

COMMON_TEST_ARGS = [
    "--blocked-reactor-notify-ms 2000000",
    "--abort-on-seastar-bad-alloc",
]


def find_vbuild_path_from_binary(binary_path, num_subdirs=1):
    """
    Return the absolute path to the vbuild directory by extracting it from a
    binary's path.

    Optionally also include num_subdirs subdirectory components.
    """
    path_parts = binary_path.split("/")
    try:
        vbuild = "/".join(path_parts[0:path_parts.index("vbuild") +
                                     num_subdirs])
    except (ValueError, IndexError):
        sys.stderr.write(
            f"Could not find vbuild in binary path {binary_path}\n")
        return
    return vbuild


class BacktraceCapture(threading.Thread):
    """
    Class for capturing stderr into a string, while also
    emitting it to out own stderr stream as it comes in,
    and then analyzing it for seastar backtraces on nonzero
    exit.
    """

    BACKTRACE_START = re.compile(
        "(^Backtrace:|.+Sanitizer.*|.+Backtrace below:$|^Direct leak.+|^Indirect leak.+|^READ of size.*|^0x.+ is located.+|^previously allocated by.+|^Thread.+created by.+|Exceptional future ignored)"
    )
    BACKTRACE_BODY = re.compile("^(  |==|0x|.*backtrace: 0x)")

    def __init__(self, binary, process):
        super(BacktraceCapture, self).__init__()
        self.process = process
        self.binary = binary

    def run(self):
        try:
            self._run()
        except Exception as e:
            logger.exception("Error capturing test stderr")

    def _run(self):
        """
        Grab blocks of lines that look like backtrace.

        Example 1: Sanitizer detects null pointer deref in a debug build

        AddressSanitizer:DEADLYSIGNAL
        =================================================================
        ==1799059==ERROR: AddressSanitizer: SEGV on unknown address 0x000000000000 (pc 0x5634332b2b5a bp 0x7fe9bd0edb70 sp 0x7fe9bd0ed5a0 T1)
        ==1799059==The signal is caused by a READ memory access.
        ==1799059==Hint: address points to the zero page.
            #0 0x5634332b2b5a  (/home/vectorized/redpanda/vbuild/debug/clang/bin/ssx_unit_rpunit+0x29e5b5a)
            #1 0x5634333064df  (/home/vectorized/redpanda/vbuild/debug/clang/bin/ssx_unit_rpunit+0x2a394df)

        Example 2: SEGV backtrace on null pointer deref in a release build

        Backtrace:
          0x2221078a
          0x2d04f624
          0x2d04f243
          0x2ce52589
          0x2ce7fbe5
        """

        blocks = []

        accumulator = None
        while True:
            line = self.process.stderr.readline()
            if line:
                sys.stderr.write(line)
                if accumulator is not None and self.BACKTRACE_BODY.search(
                        line):
                    # Mid-backtrace
                    accumulator.append(line)
                elif accumulator is not None:
                    # End of backtrace
                    if accumulator:
                        blocks.append(accumulator)
                    accumulator = None

                # A start of backtrace line, which may also have been an end of backtrace line above
                if accumulator is None and self.BACKTRACE_START.search(line):
                    accumulator = []
                    if self.BACKTRACE_BODY.search(line):
                        accumulator.append(line)
            else:
                break

        if accumulator:
            blocks.append(accumulator)

        if self.process.returncode != 0:
            # If there was at least one test failure, process the output
            # to extract Seastar backtraces, and run them through
            # seastar-addr2line to produce human readable output.
            for block in blocks:
                self._addr2lines(block)

    def _find_addr2lines(self):
        """
        seastar-addr2lines can be run directly from seastar source
        tree on developer workstations, but must be found in a magic
        location in CI, where the dependencies' sources have been
        deleted before tests are run
        :return: string, path
        """

        # CI: use prior knowledge of location where tool is
        # copied in Dockerfile for build image
        ci_location = "/vectorized/bin/seastar-addr2line"
        if os.path.exists(ci_location):
            return ci_location

        # Workstation: find our build directory by searching back from binary
        vbuild = find_vbuild_path_from_binary(self.binary, 3)
        if vbuild:
            location = os.path.join(
                vbuild,
                "v_deps_build/seastar-prefix/src/seastar/scripts/seastar-addr2line"
            )

            if not os.path.exists(location):
                sys.stderr.write(
                    f"seastar-addr2line not found at {location}\n")
                return
            else:
                return location

    def _addr2lines(self, backtrace):
        if not backtrace:
            # Maybe we saw a 'Backtrace:' line with no following
            # address lines, and got an empty list.  Ignore it.
            return

        addr2lines_path = self._find_addr2lines()
        if addr2lines_path is None:
            sys.stderr.write(
                f"Could not decode backtrace, seastar-addr2lines not found\n")
            return

        ran = subprocess.run([addr2lines_path, "-e", self.binary],
                             input="\n".join(backtrace),
                             encoding='utf-8',
                             capture_output=True)

        sys.stderr.write(f"Decoded a Seastar backtrace:\n")
        sys.stderr.write(ran.stderr)
        sys.stderr.write(ran.stdout)

        ran.check_returncode()


class TestRunner():
    def __init__(self, root, prepare_command, post_command, binary, repeat,
                 copy_files, gtest, *args):
        self.prepare_command = prepare_command
        self.post_command = post_command
        self.binary = binary
        self.repeat = repeat if repeat is not None else 1
        self.copy_files = copy_files
        self.gtest = gtest
        self.root = root
        os.makedirs(self.root, exist_ok=True)

        # make args a list
        if len(args) == 0:
            args = []
        else:
            args = list(map(str, args))

        # If in CI, run with trace because we need the evidence if something
        # fails.  Locally, use INFO to improve runtime: the developer can
        # selectively re-run failing tests with more logging if needed.
        log_level = 'trace' if self.ci else 'info'

        def has_flag(flag, *synonyms):
            """Check if the args list already contains a particularly CLI flag,
            optionally pass a list of synonyms"""
            all_flags = [flag] + list(synonyms)
            return any(any(a.startswith(f) for f in all_flags) for a in args)

        if "rpunit" in binary or "rpfixture" in binary:
            unit_args = [
                "--unsafe-bypass-fsync 1", f"--default-log-level={log_level}",
                "--logger-log-level='io=debug'",
                "--logger-log-level='exception=debug'"
            ] + COMMON_TEST_ARGS

            if self.ci:
                unit_args.append("--overprovisioned")

            # Unit tests should never need all the node's memory.  Set a fixed
            # memory size if one was not already provided
            if "rpunit" in binary and not has_flag("-m", "--memory"):
                args.append("-m1G")

            if "rpunit" in binary and not has_flag("-c", "--smp"):
                raise RuntimeError(
                    f"Test {self.binary} run without -c flag: set it in CMakeLists for the test"
                )

            if "--" in args:
                args = args + unit_args
            else:
                args = args + ["--"] + unit_args

            # gtest doesn't understand the `--` protocol
            if self.gtest:
                args = [arg for arg in args if arg != "--"]

        elif "rpbench" in binary:
            json_output = []
            vbuild = find_vbuild_path_from_binary(self.binary)
            bench_name = os.path.basename(self.binary)
            if vbuild:
                json_output = [
                    "--json-output",
                    os.path.join(vbuild, f"microbench/{bench_name}.json")
                ]

            args = args + COMMON_TEST_ARGS + json_output
        # aggregated args for test
        self.test_args = " ".join(args)

    def _gen_alphanum(self, x=16):
        return ''.join(random.choice(string.ascii_letters) for _ in range(x))

    def _gen_testdir(self):
        return tempfile.mkdtemp(suffix=self._gen_alphanum(),
                                prefix="%s/test." % self.root)

    @property
    def ci(self):
        return 'CI' in os.environ

    def run(self):
        # Execute the requested number of times, terminate on the first failure.
        for r in range(0, self.repeat):
            status = self._run()
            if status != 0:
                if self.repeat > 1:
                    logger.info(f"Failed on run {r + 1}/{self.repeat}")
                sys.exit(status)

        # Fall out of loop means status was 0 (OK).  Proceed to terminate
        # normally, no need for an explicit sys.exit on success.

        if self.repeat > 1:
            logger.info(f"Repeated {self.repeat} times, no failures")

    def _run(self):
        test_dir = self._gen_testdir()
        env = os.environ.copy()
        env["TEST_DIR"] = test_dir
        env["BOOST_TEST_LOG_LEVEL"] = "test_suite"
        if self.ci:
            env["BOOST_TEST_COLOR_OUTPUT"] = "0"
        env["BOOST_TEST_CATCH_SYSTEM_ERRORS"] = "no"
        env["BOOST_TEST_REPORT_LEVEL"] = "no"
        env["BOOST_LOGGER"] = "HRF,test_suite"
        env["UBSAN_OPTIONS"] = "halt_on_error=1:abort_on_error=1:report_error_type=1"
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
            "(cd $test_dir; $prepare_command; BOOST_TEST_LOG_LEVEL=\"test_suite\""
            " BOOST_LOGGER=\"HRF,test_suite\" $binary $args; $post_command e=$$?; "
            "rm -rf $test_dir; echo \"Test Exit code $$e\"; exit $$e)"
        ).substitute(test_dir=test_dir,
                     prepare_command=" && ".join(self.prepare_command)
                     or "true",
                     post_command=" && ".join(self.post_command) +
                     ";" if self.post_command else "",
                     binary=self.binary,
                     args=self.test_args)
        logger.info(cmd)

        # setup llvm symbolizer. first look for location in ci, then in redpanda
        # vbuild directory. if none, then asan will look in PATH
        llvm_symbolizer = shutil.which("llvm-symbolizer",
                                       path="/vectorized/llvm/bin")
        if llvm_symbolizer is None:
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "vbuild/llvm/install/bin")
            path = os.path.abspath(path)  # remove ".."
            llvm_symbolizer = shutil.which("llvm-symbolizer", path=path)
        if llvm_symbolizer is not None:
            env["ASAN_SYMBOLIZER_PATH"] = llvm_symbolizer
        logger.info(f"Using llvm-symbolizer: {llvm_symbolizer}")

        # setup lsan suppressions
        src_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               "..")
        lsan_suppressions = os.path.join(src_dir, "lsan_suppressions.txt")
        ubsan_suppressions = os.path.join(src_dir, "ubsan_suppressions.txt")
        assert os.path.isfile(
            lsan_suppressions
        ), f"cannot find lsan suppressions at {lsan_suppressions}"
        assert os.path.isfile(
            ubsan_suppressions
        ), f"cannot find ubsan suppressions at {ubsan_suppressions}"
        env["LSAN_OPTIONS"] = f"suppressions={lsan_suppressions}"
        env["UBSAN_OPTIONS"] += f":suppressions={ubsan_suppressions}"

        # We only capture stderr because that's where backtraces go
        # FIXME: avoid usage of the unsafe shell=True if possible, or sanitized the cmd input
        p = subprocess.Popen(cmd,
                             env=env,
                             shell=True,
                             stderr=subprocess.PIPE,
                             encoding='utf-8')

        def on_signal(signal, _frame):
            logger.warning(f"Passing signal {signal} to unit test binary")
            p.stderr.close()
            p.send_signal(signal)
            try:
                p.wait(5)
            except subprocess.TimeoutExpired:
                logger.warning(
                    f"Child process didn't terminate on signal {signal}")
                p.kill()

        t = BacktraceCapture(self.binary, p)

        # Pass ctrl-C etc through to the captive binary
        signal.signal(signal.SIGINT, on_signal)
        signal.signal(signal.SIGTERM, on_signal)
        t.start()
        t.join()

        return p.wait()


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
        parser.add_argument('--gtest', action='store_true')
        parser.add_argument('--root',
                            type=str,
                            default=None,
                            help="Working directory (default = cwd)")
        return parser

    parser = generate_options()
    options, program_options = parser.parse_known_args()

    if options.root is None:
        options.root = os.getcwd()

    if not options.binary:
        parser.print_help()
        exit(1)
    if not options.copy_file:
        options.copy_file = []

    logger.setLevel(getattr(logging, options.log.upper()))
    logger.info("%s *args=%s" % (options, program_options))

    runner = TestRunner(options.root, options.pre, options.post,
                        options.binary, options.repeat, options.copy_file,
                        options.gtest, *program_options)
    runner.run()
    return 0


if __name__ == '__main__':
    main()
