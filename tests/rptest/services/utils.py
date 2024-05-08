import collections
import re
import time

from abc import ABC, abstractmethod
from typing import Generator, Optional

from rptest.clients.kubectl import KubectlTool, KubeNodeShell


class Stopwatch():
    def __init__(self):
        self._start = 0
        self._end = 0

    def start(self) -> None:
        self._start = time.time()
        self._end = self._start

    def split(self) -> None:
        self._end = time.time()
        if self._start == 0:
            self._start = self._end

    @property
    def elapsed(self) -> float:
        """Calculates time elapsed from _start to _end.
        If stop method is not called, returns current elapsed time

        Returns:
            float: elapsed time
        """
        if self._end != self._start:
            return self._end - self._start
        else:
            return time.time() - self._start

    def elapseds(self) -> str:
        return f"{self.elapsed:.2f}s"

    def elapsedf(self, note) -> str:
        return f"{note}: {self.elapseds()}"


class BadLogLines(Exception):
    def __init__(self, node_to_lines):
        self.node_to_lines = node_to_lines

    def __str__(self):
        # Pick the first line from the first node as an example, and include it
        # in the string output so that for single line failures, it isn't necessary
        # for folks to search back in the log to find the culprit.
        example_lines = next(iter(self.node_to_lines.items()))[1]
        example = next(iter(example_lines))

        summary = ','.join([
            f'{i[0].account.hostname}({len(i[1])})'
            for i in self.node_to_lines.items()
        ])
        return f"<BadLogLines nodes={summary} example=\"{example}\">"

    def __repr__(self):
        return self.__str__()


class NodeCrash(Exception):
    def __init__(self, crashes):
        self.crashes = crashes

        # Not legal to construct empty
        assert len(crashes)

    def __str__(self):
        example = f"{self.crashes[0][0].name}: {self.crashes[0][1]}"
        if len(self.crashes) == 1:
            return f"<NodeCrash {example}>"
        else:
            names = ",".join([c[0].name for c in self.crashes])
            return f"<NodeCrash ({names}) {example}>"

    def __repr__(self):
        return self.__str__()


class LogSearch(ABC):
    # globals key
    RAISE_ON_ERRORS_KEY = "raise_on_error"

    # Largest allocation allowed in during a test
    MAX_ALLOCATION_SIZE = 400 * 1024  # 400KiB

    DEFAULT_MATCH_TERMS = [
        "Segmentation fault",
        "[Aa]ssert",
        "Exceptional future ignored",
        "UndefinedBehaviorSanitizer",
        "Aborting on shard",
        "libc++abi: terminating due to uncaught exception",
        "oversized allocation",
    ]

    def __init__(self, test_context, allow_list, logger) -> None:
        self._context = test_context
        self.allow_list = allow_list
        self.logger = logger
        self._raise_on_errors = self._context.globals.get(
            self.RAISE_ON_ERRORS_KEY, True)

        # Prepare matching terms
        self.match_terms = self.DEFAULT_MATCH_TERMS
        if self._raise_on_errors:
            self.match_terms.append("^ERROR")
        self.match_expr = " ".join(f"-e \"{t}\"" for t in self.match_terms)

    @abstractmethod
    def _capture_log(self, x, s) -> Generator[str, None, None]:
        """Method to get log from host node. Overriden by each child.
        """
        # Fake return type for type hint silence
        # And proper handling when called directly
        for x in []:
            yield x

    @abstractmethod
    def _get_hostname(self, host) -> str:
        """Method to get name of the host. Overriden by each child.
        """
        return ""

    def _check_if_line_allowed(self, line):
        for a in self.allow_list:
            if a.search(line) is not None:
                self.logger.warn(f"Ignoring allow-listed log line '{line}'")
                return True
        return False

    def _check_memory_leak(self, host):
        # Special case for LeakSanitizer errors, where tiny leaks
        # are permitted, as they can occur during Seastar shutdown.
        # See https://github.com/redpanda-data/redpanda/issues/3626
        for summary_line in self._capture_log(host,
                                              "SUMMARY: AddressSanitizer:"):
            m = re.match(
                "SUMMARY: AddressSanitizer: (\d+) byte\(s\) leaked in (\d+) allocation\(s\).",
                summary_line.strip())
            if m and int(m.group(1)) < 1024:
                self.logger.warn(
                    f"Ignoring memory leak, small quantity: {summary_line}")
                return True
        return False

    def _check_oversized_allocations(self, line):
        m = re.search("oversized allocation: (\d+) byte", line)
        if m and int(m.group(1)) <= self.MAX_ALLOCATION_SIZE:
            self.logger.warn(
                f"Ignoring oversized allocation, {m.group(1)} is less than the max allowable allocation size of {self.MAX_ALLOCATION_SIZE} bytes"
            )
            return True
        return False

    def _search(self, nodes):
        bad_lines = collections.defaultdict(list)
        test_name = self._context.function_name
        sw = Stopwatch()
        for node in nodes:
            sw.start()
            hostname = self._get_hostname(node)
            self.logger.info(f"Scanning node {hostname} log for errors...")
            # Prepare/Build capture func shortcut
            # Iterate
            for line in self._capture_log(node, self.match_expr):
                line = line.strip()
                # Check if this line holds error
                allowed = self._check_if_line_allowed(line)
                # Check for memory leaks
                if 'LeakSanitizer' in line:
                    allowed = self._check_memory_leak(node)
                # Check for oversized allocations
                if "oversized allocation" in line:
                    allowed = self._check_oversized_allocations(line)
                # If detected bad lines, log it and add to the list
                if not allowed:
                    bad_lines[node].append(line)
                    self.logger.warn(f"[{test_name}] Unexpected log line on "
                                     f"{hostname}: {line}")
            self.logger.info(
                sw.elapsedf(
                    f"##### Time spent to scan bad logs on '{hostname}'"))
        return bad_lines

    def search_logs(self, nodes):
        # Do log search
        bad_loglines = self._search(nodes)
        # If anything, raise exception
        if bad_loglines:
            # Call class overriden method to get proper Exception class
            raise BadLogLines(bad_loglines)


class LogSearchLocal(LogSearch):
    def __init__(self, test_context, allow_list, logger, targetpath) -> None:
        super().__init__(test_context, allow_list, logger)
        self.targetpath = targetpath

    def _capture_log(self, node, expr) -> Generator[str, None, None]:
        cmd = f"grep {expr} {self.targetpath} || true"
        for line in node.account.ssh_capture(cmd):
            yield line

    def _get_hostname(self, host) -> str:
        return host.account.hostname


class LogSearchCloud(LogSearch):
    def __init__(self, test_context, allow_list, logger, kubectl: KubectlTool,
                 test_start_time) -> None:
        super().__init__(test_context, allow_list, logger)

        # Prepare capture functions
        self.kubectl = kubectl
        self.test_start_time = test_start_time

    def _capture_log(self, pod, expr) -> Generator[str, None, None]:
        """Capture log and check test timing.
           If logline produced before test start, ignore it
        """
        def parse_k8s_time(logline, tz):
            k8s_time_format = "%Y-%m-%dT%H:%M:%S.%f %z"
            # containerd has nanoseconds format (9 digits)
            # python supports only 6
            logline_time = logline.split()[0]
            # find '.' (dot) and cut at 6th digit
            logline_time = f"{logline_time[:logline_time.index('.')+7]} {tz}"
            return time.strptime(logline_time, k8s_time_format)

        # Load log, output is in binary form
        loglines = []
        tz = "+00:00"
        try:
            # get time zone in +00:00 format
            tz = pod.nodeshell("date +'%:z'")
            # Assume UTC if output is empty
            # But this should never happen
            tz = tz[0] if len(tz) > 0 else "+00:00"
            # Find all log files for target pod
            # Return type without capture is always str, so ignore type
            logfiles = pod.nodeshell(
                f"find /var/log/pods -type f")  # type: ignore
            for logfile in logfiles:
                if pod.name in logfile and \
                        'redpanda-configurator' not in logfile:  # type: ignore
                    self.logger.info(f"Inspecting '{logfile}'")
                    lines = pod.nodeshell(f"cat {logfile} | grep {expr}")
                    loglines += lines
        except Exception as e:
            self.logger.warning(f"Error getting logs for {pod.name}: {e}")
        else:
            _size = len(loglines)
            self.logger.debug(f"Received {_size}B of data from {pod.name}")

        # check log lines for proper timing.
        # Log lines will have two timing objects:
        # first - when K8s received the log, second - when RP actually generated that log line.
        # These two will differ as containerd/k8s buffers data. So since there is little
        # chance that errors would generate exactly at the start of the test, we safely using
        # time from K8s as it will be consistent no matter which service it comes from

        # Example logline
        # '2024-04-11T17:05:13.758476896Z stderr F WARN  2024-04-11 17:05:13,755 [shard 0:main] seastar_memory - oversized allocation: 217088 bytes. This is non-fatal, but could lead to latency and/or fragmentation issues. Please report: at 0x80ddafb 0x7de622b 0x7df04bf /opt/redpanda/lib/libgnutls.so.30+0xc5ca3 /opt/redpanda/lib/libgnutls.so.30+0x12a9e3 /opt/redpanda/lib/libgnutls.so.30+0x813df 0x80906ef 0x7f66333'

        # Iterate lines and return generator
        for line in loglines:
            logline_time = parse_k8s_time(line, tz)
            test_start_time = time.gmtime(self.test_start_time)
            if logline_time < test_start_time:
                continue
            yield line

    def _get_hostname(self, host) -> str:
        return host.hostname
