import collections
import re

from abc import ABC, abstractmethod
from typing import Generator, Optional

from rptest.clients.kubectl import KubectlTool, KubeNodeShell


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
        for node in nodes:
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
        return bad_lines

    def search_logs(self, nodes):
        # Do log search
        bad_loglines = self._search(nodes)
        # If anything, raise exception
        if bad_loglines:
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
    def __init__(self, test_context, allow_list, logger,
                 kubectl: KubectlTool) -> None:
        super().__init__(test_context, allow_list, logger)

        # Prepare capture functions
        self.kubectl = kubectl

    def _capture_log(self, pod, expr) -> Generator[str, None, None]:
        # Load log, output is in binary form
        loglines = []
        pod_name = self._get_hostname(pod)
        node_name = pod['spec']['nodeName']
        with KubeNodeShell(self.kubectl, node_name) as ksh:
            try:
                logfiles = ksh(f"find /var/log/pods -type f")
                for logfile in logfiles:
                    if pod_name in logfile and \
                        'redpanda-configurator' not in logfile:
                        self.logger.info(f"Inspecting '{logfile}'")
                        lines = ksh(f"cat {logfile} | grep {expr}")
                        loglines += lines
            except Exception as e:
                self.logger.warning(f"Error getting logs for {pod_name}: {e}")
            else:
                _size = len(loglines)
                self.logger.debug(f"Received {_size}B of data from {pod_name}")

        for line in loglines:
            yield line

    def _get_hostname(self, host) -> str:
        return host['metadata']['name']
