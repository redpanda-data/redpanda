from logging import Logger
from pathlib import Path
import re
import shutil
import time
import os
from abc import ABC, abstractmethod
from typing import Any, Generator, Iterable, TypedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

from rptest.clients.kubectl import KubectlTool

from ducktape.tests.test import TestContext
from ducktape.cluster.cluster import ClusterNode

from rptest.services.cloud_broker import CloudBroker
from rptest.services.redpanda_types import CompiledLogAllowList

VersionedNodes = Iterable[tuple[str | None, ClusterNode | CloudBroker]]


class VersionAndLines(TypedDict):
    version: str | None
    lines: list[str]


NodeToLines = dict[ClusterNode | CloudBroker, VersionAndLines]


def assert_int(v: Any) -> int:
    assert isinstance(v, int), f"not an int: {v}"
    return v


def assert_int_or_none(v: Any) -> int | None:
    assert v is None or isinstance(v, int), f"not an int or none: {v}"
    return v


class Stopwatch:
    def __init__(self) -> None:
        self._start: float = 0
        self._end: float = 0

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
        return f"{self.elapsed:.3f}s"

    def elapsedf(self, note: str) -> str:
        return f"{note}: {self.elapseds()}"


class BadLogLines(Exception):
    def __init__(self, node_to_lines: NodeToLines) -> None:
        self.node_to_lines = node_to_lines
        self._str = self._make_str(node_to_lines)

    @staticmethod
    def _make_str(node_to_lines: NodeToLines) -> str:
        # Pick the first line from the first node as an example, and include it
        # in the string output so that for single line failures, it isn't necessary
        # for folks to search back in the log to find the culprit.
        example_lines = next(iter(node_to_lines.items()))[1]
        example = next(iter(example_lines["lines"]))

        summary_list: list[str] = []
        for i in node_to_lines.items():
            version_or_none = i[1]["version"]
            node_info = (
                f"<{i[0].account.hostname}:{version_or_none}>"
                if version_or_none is not None
                else f"{i[0].account.hostname}"
            )
            summary_list.append(f"{node_info}({len(i[1])})")
        summary = ",".join(summary_list)
        return f'<BadLogLines nodes={summary} example="{example}">'

    def __str__(self) -> str:
        return self._str

    def __repr__(self) -> str:
        return self.__str__()


class NodeCrash(Exception):
    def __init__(self, crashes: list[tuple[Any, str]]) -> None:
        self.crashes = crashes

        # Not legal to construct empty
        assert len(crashes)

    def __str__(self) -> str:
        example = f"{self.crashes[0][0].name}: {self.crashes[0][1]}"
        if len(self.crashes) == 1:
            return f"<NodeCrash {example}>"
        else:
            names = ",".join([c[0].name for c in self.crashes])
            return f"<NodeCrash ({names}) {example}>"

    def __repr__(self) -> str:
        return self.__str__()


class LogSearch(ABC):
    # globals key
    RAISE_ON_ERRORS_KEY = "raise_on_error"

    # Largest allocation allowed in during a test
    MAX_ALLOCATION_SIZE = 200 * 1024  # 200KiB

    DEFAULT_MATCH_TERMS = [
        "Segmentation fault",
        "[Aa]ssert",
        "Exceptional future ignored",
        "UndefinedBehaviorSanitizer",
        "Aborting on shard",
        "terminating due to uncaught exception",
        "oversized allocation",
    ]

    def __init__(
        self,
        test_context: TestContext,
        allow_list: CompiledLogAllowList,
        logger: Logger,
    ) -> None:
        self._context = test_context
        self.allow_list = allow_list
        self.logger = logger
        self._raise_on_errors: bool = self._context.globals.get(
            self.RAISE_ON_ERRORS_KEY, True
        )
        self._max_workers: int | None = None

        # Prepare matching terms
        self.match_terms: list[str] = list(self.DEFAULT_MATCH_TERMS)
        if self._raise_on_errors:
            self.match_terms.append("^ERROR")
        self.match_expr: str = " ".join(f'-e "{t}"' for t in self.match_terms)

    @abstractmethod
    def _capture_log(self, node: Any, expr: str) -> Generator[str, None, None]:
        """Method to get log from host node. Overriden by each child.

        expr is a GNU BRE regex (i.e., the default grep regex style), which means
        you need to escape things like +() if you intend them to be metacharacters"""
        # Fake return type for type hint silence
        # And proper handling when called directly
        yield from []

    @abstractmethod
    def _get_hostname(self, host: Any) -> str:
        """Method to get name of the host. Overriden by each child."""
        return ""

    def _check_if_line_allowed(self, line: str) -> bool:
        for a in self.allow_list:
            if a.search(line) is not None:
                self.logger.info(f"Ignoring allow-listed log line '{line}'")
                return True
        return False

    def _check_memory_leak(self, host: Any) -> bool:
        # Special case for LeakSanitizer errors, where tiny leaks
        # are permitted, as they can occur during Seastar shutdown.
        # See https://github.com/redpanda-data/redpanda/issues/3626
        for summary_line in self._capture_log(host, "SUMMARY: AddressSanitizer:"):
            m = re.match(
                r"SUMMARY: AddressSanitizer: (\d+) byte\(s\) leaked in (\d+) allocation\(s\).",
                summary_line.strip(),
            )
            if m and int(m.group(1)) < 1024:
                self.logger.warning(
                    f"Ignoring memory leak, small quantity: {summary_line}"
                )
                return True
        return False

    def _check_oversized_allocations(self, line: str) -> bool:
        m = re.search(r"oversized allocation: (\d+) byte", line)
        if m and int(m.group(1)) <= self.MAX_ALLOCATION_SIZE:
            self.logger.warning(
                f"Ignoring oversized allocation, {m.group(1)} is less than the max allowable allocation size of {self.MAX_ALLOCATION_SIZE} bytes"
            )
            return True
        return False

    def _search(self, versioned_nodes: VersionedNodes) -> NodeToLines:
        test_name = self._context.function_name
        overall_sw = Stopwatch()
        overall_sw.start()

        def scan_one(
            version: str | None, node: ClusterNode | CloudBroker
        ) -> tuple[ClusterNode | CloudBroker, VersionAndLines]:
            node_sw = Stopwatch()
            node_sw.start()
            hostname = self._get_hostname(node)
            self.logger.info(f"Scanning node {hostname} log for errors...")
            vl: VersionAndLines = {"version": version, "lines": []}
            for line in self._capture_log(node, self.match_expr):
                line = line.strip()
                # Check if this line holds error
                allowed = self._check_if_line_allowed(line)
                # Check for memory leaks
                if "LeakSanitizer" in line:
                    allowed = self._check_memory_leak(node)
                # Check for oversized allocations
                if "oversized allocation" in line:
                    allowed = self._check_oversized_allocations(line)
                # If detected bad lines, log it and add to the list
                if not allowed:
                    vl["lines"].append(line)
                    self.logger.warning(
                        f"[{test_name}] Unexpected log line on {hostname}: {line}"
                    )
            self.logger.info(
                node_sw.elapsedf(f"Time spent to scan bad logs on '{hostname}'")
            )
            return node, vl

        bad_lines: NodeToLines = {}

        # Run scans in parallel
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = [executor.submit(scan_one, v, n) for v, n in versioned_nodes]
            for fut in as_completed(futures):
                node, vl = fut.result()
                if vl["lines"]:
                    bad_lines[node] = vl

        self.logger.info(overall_sw.elapsedf("Time spent to scan bad logs overall"))
        return bad_lines

    def search_logs(self, versioned_nodes: VersionedNodes) -> None:
        """
        versioned_nodes is a list of Tuple[version, node]
        """
        # Do log search
        bad_loglines = self._search(versioned_nodes)
        # If anything, raise exception
        if bad_loglines:
            # Call class overriden method to get proper Exception class
            raise BadLogLines(bad_loglines)


def _gnu_bre_to_ere(bre_pattern: str) -> str:
    r"""
    Convert a GNU Basic Regular Expression (BRE) to a GNU Extended Regular
    Expression (ERE).

    This function handles two main differences between GNU BRE and ERE:
    1.  In BRE, `(`, `)`, `{`, `}`, `+`, `?`, and `|` are literal characters,
        whereas in ERE they are special metacharacters. To treat them as
        literals in ERE, they must be escaped with a backslash.
    2.  In BRE, the escaped versions `\(`, `\)`, `\{`, `\}`, `\+`, `\?`, and
        `\|` have special meanings (grouping, intervals, etc.), while in ERE,
        the unescaped versions have these special meanings.

    The conversion is performed by iterating through the BRE pattern and
    applying the following rules:
    - Unescaped `(`, `)`, `{`, `}`, `+`, `?`, `|` are escaped.
    - Escaped `\(`, `\)`, `\{`, `\}`, `\+`, `\?`, `\|` are unescaped.
    - Other characters, including other escaped characters (e.g., `\.`, `\*`),
      are kept as they are.
    - The logic correctly handles double backslashes (`\\`), ensuring they
      are preserved.
    """

    # these are metacharacters in both ERE and GNU BRE but in BRE
    # they must be escaped to have their metacharacter meaning
    BRE_ESCAPED_METACHARACTERS = set("(){}+?|")

    ere_pattern = ""
    i = 0
    while i < len(bre_pattern):
        char = bre_pattern[i]
        if char == "\\":
            if i + 1 < len(bre_pattern):
                next_char = bre_pattern[i + 1]
                if next_char in BRE_ESCAPED_METACHARACTERS:
                    # Unescape BRE metacharacters to become ERE metacharacters
                    ere_pattern += next_char
                    i += 2
                else:
                    # Keep other escaped characters as they are (e.g., \\, \*, \.)
                    ere_pattern += char + next_char
                    i += 2
            else:
                # Trailing backslash
                ere_pattern += char
                i += 1
        elif char in BRE_ESCAPED_METACHARACTERS:
            # Escape ERE metacharacters that are literals in BRE
            ere_pattern += "\\" + char
            i += 1
        else:
            # Keep all other characters
            ere_pattern += char
            i += 1
    return ere_pattern


class LogSearchLocal(LogSearch):
    def __init__(
        self,
        test_context: TestContext,
        allow_list: CompiledLogAllowList,
        logger: Logger,
        targetpath: str,
    ) -> None:
        super().__init__(test_context, allow_list, logger)
        self.targetpath = targetpath

    def _capture_log(self, node: ClusterNode, expr: str) -> Generator[str, None, None]:
        if not expr.startswith("-P"):
            # some naughty tests use this to force grep/rg to use PRCE
            expr = _gnu_bre_to_ere(expr)
        cmd = f"rg {expr} {self.targetpath} || true"
        for line in node.account.ssh_capture(cmd):
            yield line

    def _get_hostname(self, host: ClusterNode) -> str:
        return host.account.hostname


class LogSearchCloud(LogSearch):
    def __init__(
        self,
        test_context: TestContext,
        allow_list: CompiledLogAllowList,
        logger: Logger,
        kubectl: KubectlTool,
        test_start_time: float,
    ) -> None:
        super().__init__(test_context, allow_list, logger)

        # Prepare capture functions
        self.kubectl = kubectl
        self.test_start_time = test_start_time
        # Cloudv2 agents run on very small instances that can easily be
        # overwhelmed by too many concurrent ssh sessions.
        self._max_workers = 10

    def _capture_log(self, node: CloudBroker, expr: str) -> Generator[str, None, None]:
        """Capture log and check test timing.
        If logline produced before test start, ignore it
        """

        def parse_k8s_time(logline: str, tz: str) -> time.struct_time:
            k8s_time_format = "%Y-%m-%dT%H:%M:%S.%f %z"
            # containerd has nanoseconds format (9 digits)
            # python supports only 6
            logline_time = logline.split()[0]
            # find '.' (dot) and cut at 6th digit
            logline_time = f"{logline_time[: logline_time.index('.') + 7]} {tz}"
            return time.strptime(logline_time, k8s_time_format)

        # Load log, output is in binary form
        loglines: list[str] = []
        tz: str = "+00:00"
        try:
            # get time zone in +00:00 format
            # nodeshell return type is not well-typed, cast result to list[str]
            tz_result = node.nodeshell("date +'%:z'")
            # Assume UTC if output is empty
            # But this should never happen
            if isinstance(tz_result, list) and len(tz_result) > 0:
                tz = str(tz_result[0])
            # Find all log files for target pod
            # Return type without capture is always str, so ignore type
            logfiles_result = node.nodeshell("find /var/log/pods -type f")
            logfiles = (
                [str(f) for f in logfiles_result]
                if isinstance(logfiles_result, list)
                else []
            )
            for logfile in logfiles:
                if node.name in logfile and "redpanda-configurator" not in logfile:
                    self.logger.info(f"Inspecting '{logfile}'")
                    lines_result = node.nodeshell(
                        f"cat {logfile} | grep {expr} || [[ $? == 1 ]]"
                    )
                    lines = (
                        [str(line) for line in lines_result]
                        if isinstance(lines_result, list)
                        else []
                    )
                    loglines += lines
        except Exception as e:
            self.logger.warning(f"Error getting logs for {node.name}: {e}")
        else:
            _size = len(loglines)
            self.logger.debug(f"Received {_size}B of data from {node.name}")

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

    def _get_hostname(self, host: CloudBroker) -> str:
        return host.hostname


class LocalPayloadDirectory:
    def __init__(self, path: Path = Path("/tmp/custom_payloads")):
        """
        Used to create/maintain a local directory of payloads on the ducktape runner node.
        Note that each payload needs to be the same size.

        :param path: used to specify the path on the localhost where custom
                     payload files are stored.
        """
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=False, exist_ok=False)
        self.path = path
        self.payload_size: None | int = None

    def __del__(self):
        shutil.rmtree(self.path)

    def add_payload(self, payload_name: str, payload: bytes):
        """
        Creates and populates a file in the local payload dir.

        :param payload_name: Name of file to be created.
        :param payload: Contents of file to be created.
        """
        if self.payload_size is None:
            self.payload_size = len(payload)

        assert len(payload) == self.payload_size, (
            "all custom payloads must be the same size"
        )

        with open(self.path / f"{payload_name}.data", "wb") as f:
            f.write(payload)

    def has_payloads(self) -> bool:
        return self.payload_size is not None

    def copy_to_node(self, node: ClusterNode, dst: str):
        """
        Copies local payload dir to a remote node.

        :param node: The remote node the local dir will be copied to.
        :param dst: Location in the remote node that the local dir will be copied to.
                    Note that there must not be a dir already in the remote node at
                    this location already.
        """
        local_payload_dir = self.path

        payload_file_list: list[str] = []
        for file in os.listdir(local_payload_dir):
            file_path = os.path.join(local_payload_dir, file)
            if file.endswith("data") and not os.path.isdir(file_path):
                payload_file_list.append(file_path)

        na = node.account
        assert not na.exists(dst), (
            f"Custom payload dir {dst} already exists on node {node}."
        )

        na.mkdirs(dst)
        for payload_file in payload_file_list:
            na.copy_to(payload_file, dst)
