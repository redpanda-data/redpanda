#!/usr/bin/env python3

# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

newline = "\n"
tab = "\t"
newlinetab = "\n\t"

just_cfr_mode_description = """just controller forced reconfiguration
python3 invoke_controller_reconfiguration.py baremetal --nodes IP1 IP2 IP3 \\
  --dead-nodes 1 2 3 --surviving-count 2
- the script will send a controller forced reconfiguration to
  every ip provided with dead nodes and surviving count as given
"""

baremetal_with_liveness_description = """try to generate a command
python3 invoke_controller_reconfiguration.py baremetal --nodes IP1 IP2 IP3 --check-liveness
- the script will call request node status information
  from all provided IPs (usually the IPs of all nodes alive or dead)
- it will try to compile the list of dead nodes and the surviving node count
- if the plan generation is successful, it will ask the operator for permission to dispatch the CFR requests
"""

kubernetes_description = """execute against a kubernetes cluster
- precondition: kubectl must be configured against the cluster in question
  tsh kube login --proxy=<proxy> <cluster_id>
- invoke_controller_reconfiguration will use this to target the kubernetes cluster
  python3 invoke_controller_reconfiguration.py kubernetes
- the script will scrape redpanda pod details from kubectl
- it will setup tunnels to each pod's admin api
- it will run controller forced reconfiguration with liveness check populating 'nodes' with the port forwards"""

description = f"""
Helper script to orchestrate invoking Controller Forced Reconfiguration.

This script has three modes
    1. just invoke controller forced reconfiguration
    2. try to generate a controller forced reconfiguration command set and ask to run it
    3. execute against a kubernetes cluster

Usage:
    1. {newlinetab.join(just_cfr_mode_description.split(newline))}

    2. {newlinetab.join(baremetal_with_liveness_description.split(newline))}

    3. {newlinetab.join(kubernetes_description.split(newline))}
"""

CFR_API_SUFFIX = (
    "redpanda.core.admin.internal.v1.BreakglassService/ControllerForcedReconfiguration"
)

# URL and API constants
BROKERS_API_PATH = "/v1/brokers"

DEFAULT_LOCAL_PORT_BINDING_START = 27900
DEFAULT_ADMIN_PORT = 9644
DEFAULT_KUBERNETES_NAMESPACE = "redpanda"
DEFAULT_KUBERNETES_POD_PREFIX = "rp-"
CONSOLE_BAR = "=" * 70

from dataclasses import dataclass
from subprocess import CompletedProcess
from typing import List, Optional

import argparse
import atexit
import json
import logging
import select
import signal
import subprocess
import sys
import threading
import time
import requests

logger = logging.getLogger(__name__)

"""types and then globals defined with those types"""


@dataclass
class TLSConfig:
    """TLS configuration for HTTPS connections."""

    ca_cert: Optional[str] = None  # Path to CA certificate/bundle
    client_cert: Optional[str] = None  # Path to client certificate
    client_key: Optional[str] = None  # Path to client private key

    @property
    def enabled(self) -> bool:
        """TLS is enabled if any TLS configuration is provided."""
        return any([self.ca_cert, self.client_cert, self.client_key])

    def get_request_args(self) -> dict:
        """Get arguments for requests library based on TLS config."""
        args = {}

        if not self.enabled:
            return args

        # Certificate verification
        if self.ca_cert:
            args["verify"] = self.ca_cert
        else:
            args["verify"] = True  # Use system CA bundle

        # Client certificate for mutual TLS
        if self.client_cert:
            if self.client_key:
                args["cert"] = (self.client_cert, self.client_key)
            else:
                args["cert"] = self.client_cert  # Assumes cert and key in same file

        return args

    @property
    def scheme(self) -> str:
        """Return the URL scheme based on TLS configuration."""
        return "https" if self.enabled else "http"

    def __str__(self) -> str:
        """Return a string representation of the TLS configuration."""
        if not self.enabled:
            return "TLS disabled: Using HTTP"

        lines = ["TLS enabled: Using HTTPS"]
        if self.ca_cert:
            lines.append(f"  CA certificate: {self.ca_cert}")
        if self.client_cert:
            lines.append(f"  Client certificate: {self.client_cert}")
        if self.client_key:
            lines.append(f"  Client key: {self.client_key}")
        return "\n".join(lines)


# Global TLS configuration (defaults to disabled)
_tls_config = TLSConfig()


@dataclass
class BasicAuthConfig:
    """Basic authentication configuration for HTTP/HTTPS connections."""

    username: Optional[str] = None
    password: Optional[str] = None

    @property
    def enabled(self) -> bool:
        """Basic auth is enabled if username is provided."""
        return self.username is not None

    def get_auth(self) -> Optional[tuple]:
        """Get auth tuple for requests library."""
        if self.enabled:
            return (self.username, self.password or "")
        return None

    def __str__(self) -> str:
        """Return a string representation of the basic auth configuration."""
        if not self.enabled:
            return "Basic Auth disabled"
        return f"Basic Auth enabled: Username: {self.username}"


# Global Basic Auth configuration (defaults to disabled)
_basic_auth_config = BasicAuthConfig()


@dataclass
class PortForward:
    """Represents a kubectl port-forward connection."""

    local_port: int
    process: subprocess.Popen
    admin_port: int

    def __str__(self) -> str:
        return f"PortForward(local={self.local_port}, admin={self.admin_port}, pid={self.process.pid})"

    def is_alive(self) -> bool:
        """Check if the port forward process is still running."""
        return self.process.poll() is None

    def terminate(self, timeout: float = 2.0) -> None:
        """Terminate the port forward process gracefully, then forcefully if needed."""
        if not self.is_alive():
            return

        self.process.terminate()
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


# Global list to track active port forwards for cleanup
_active_port_forwards: list[PortForward] = []


def cleanup_port_forwards() -> None:
    """Clean up all active port forwards on exit."""
    global _active_port_forwards
    if _active_port_forwards:
        logger.debug("cleaning up port forwards")
        for pf in _active_port_forwards:
            try:
                pf.terminate(timeout=2.0)
            except Exception as e:
                # Log errors during cleanup for debugging
                logger.error(
                    f"failed to cleanup port forward - port: {pf.local_port}, "
                    f"PID: {pf.process.pid}, "
                    f"error: {str(e)}"
                )
        _active_port_forwards = []
        logger.debug("cleaned up port forwards")


def signal_handler(signum: int, _) -> None:
    """Handle interrupt signals by cleaning up port forwards and exiting."""
    logger.debug(f"signal {signum} caught")
    cleanup_port_forwards()
    sys.exit(1)


# Register cleanup handlers
atexit.register(cleanup_port_forwards)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

"""just types"""


@dataclass(frozen=True)
class RedpandaAdminEndpoint:
    """Represents a Redpanda node's admin API endpoint."""

    ip: str
    port: int

    def __str__(self) -> str:
        return f"{self.ip}:{self.port}"


@dataclass(frozen=True)
class LivenessCheckResult:
    """Success result of a single liveness checker"""

    alive_nodes: set[int]
    dead_nodes: set[int]
    all_node_ids: set[int]
    responding_endpoint: RedpandaAdminEndpoint

    def __str__(self) -> str:
        return json.dumps(
            {
                "alive_nodes": sorted(list(self.alive_nodes)),
                "dead_nodes": sorted(list(self.dead_nodes)),
                "all_node_ids": sorted(list(self.all_node_ids)),
                "responding_endpoint": str(self.responding_endpoint),
            },
            indent=2,
        )


@dataclass
class LivenessCheckError:
    """Error result for a single liveness checker"""

    error_str: str
    error_endpoint: RedpandaAdminEndpoint

    def __str__(self) -> str:
        return json.dumps(
            {"error": self.error_str, "endpoint": str(self.error_endpoint)}, indent=2
        )


@dataclass
class LivenessCheckResults:
    """Collected results of numerous liveness checkers, used to make CFR plan"""

    alive_nodes: set[int]
    dead_nodes: set[int]
    all_node_ids: set[int]
    responding_endpoints: set[RedpandaAdminEndpoint]

    def __str__(self) -> str:
        return json.dumps(
            {
                "alive_nodes": sorted(list(self.alive_nodes)),
                "dead_nodes": sorted(list(self.dead_nodes)),
                "all_node_ids": sorted(list(self.all_node_ids)),
                "responding_endpoints": [
                    str(ep)
                    for ep in sorted(
                        self.responding_endpoints, key=lambda e: (e.ip, e.port)
                    )
                ],
            },
            indent=2,
        )


@dataclass
class CFRParams:
    """Parameters needed for Controller Forced Reconfiguration."""

    dead_node_ids: set[int]
    surviving_count: int
    responding_endpoints: set[RedpandaAdminEndpoint]

    def __str__(self) -> str:
        return json.dumps(
            {
                "dead_node_ids": sorted(list(self.dead_node_ids)),
                "surviving_count": self.surviving_count,
                "responding_endpoints": [
                    str(ep)
                    for ep in sorted(
                        self.responding_endpoints, key=lambda e: (e.ip, e.port)
                    )
                ],
            },
            indent=2,
        )


class LivenessChecker:
    """Scrapes a given node's living/dead nodes from /v1/brokers API."""

    def __init__(self, endpoint: RedpandaAdminEndpoint):
        # set on construction
        self.endpoint = endpoint

        # set on request
        # raw json payload
        self.brokers_data = None
        # truthy error implies unsuccessful
        self.error = None

    def check(self) -> None:
        """do the liveness check and split into success or error"""
        global _tls_config, _basic_auth_config
        url = f"{_tls_config.scheme}://{self.endpoint.ip}:{self.endpoint.port}{BROKERS_API_PATH}"

        logger.info(f"[{self.endpoint}] starting liveness check")

        logger.debug(f"[{self.endpoint}] tls args: {_tls_config}")
        request_args = _tls_config.get_request_args()
        logger.debug(f"[{self.endpoint}] auth args {_basic_auth_config}")
        auth = _basic_auth_config.get_auth()
        if auth:
            request_args["auth"] = auth

        try:
            response = requests.get(url, timeout=10, **request_args)

            if response.status_code == 200:
                self.brokers_data = response.json()
            else:
                self.error = f"HTTP {response.status_code}: {response.text}"
        except requests.exceptions.Timeout:
            self.error = "Request timeout"
        except requests.exceptions.ConnectionError as e:
            self.error = f"Connection error: {str(e)}"
        except Exception as e:
            self.error = f"Unexpected error: {str(e)}"
        if self.error:
            logger.debug(f"[{self.endpoint}] finished with error: {self.error}")
            return
        logger.debug(
            f"[{self.endpoint}] finished successfully with payload: {self.brokers_data}"
        )

    def yield_result(self) -> LivenessCheckResult | LivenessCheckError:
        """parse out a success response or hand back the erorr string"""
        if self.error:
            return LivenessCheckError(
                error_str=self.error, error_endpoint=self.endpoint
            )

        if not self.brokers_data:
            raise RuntimeError("a valid response must contain a brokers_data")

        # Extract alive nodes
        alive_nodes = set(
            broker["node_id"]
            for broker in self.brokers_data
            if broker.get("is_alive", False)
        )
        dead_nodes = set(
            broker["node_id"]
            for broker in self.brokers_data
            if not broker.get("is_alive", False)
        )
        if alive_nodes.intersection(dead_nodes):
            raise RuntimeError(
                f"Data inconsistency: nodes {alive_nodes & dead_nodes} marked as both alive and dead"
            )

        return LivenessCheckResult(
            alive_nodes=alive_nodes,
            dead_nodes=dead_nodes,
            all_node_ids=set(alive_nodes).union(dead_nodes),
            responding_endpoint=self.endpoint,
        )


class CFRInvoker:
    """Responsible for invoking cfr api on a single node (endpoint) and yielding the result"""

    def __init__(
        self,
        endpoint: RedpandaAdminEndpoint,
        dead_node_ids: List[int],
        surviving_node_count: int,
    ):
        self.endpoint = endpoint
        self.dead_node_ids = dead_node_ids
        self.surviving_node_count = surviving_node_count
        self.result = None
        self.error = None

    def _was_request_successful(self, response: requests.Response) -> bool:
        """
        CFR refuses to run if theres already a controller leader.

        Since the CFR api allows the node it lands on to vote for itself, its possible for
        a CFR operation to land on a valid leader (tied for best log) and that node to elect itself leader,
        while another lands after a leader has already been chose and fails on "theres already a leader".

        These 'errors' can simply be ignored
        """
        return (
            response.status_code in [200, 201, 204]
            or "use the existing controller leader" in response.text
        )

    def invoke(self) -> None:
        global _tls_config, _basic_auth_config
        url = f"{_tls_config.scheme}://{self.endpoint.ip}:{self.endpoint.port}/{CFR_API_SUFFIX}"
        payload = {
            "dead_node_ids": self.dead_node_ids,
            "surviving_node_count": self.surviving_node_count,
        }

        logger.info(
            f"[{self.endpoint}] sending request to {url}, payload: {json.dumps(payload)}"
        )
        request_args = _tls_config.get_request_args()
        logger.debug(f"[{self.endpoint}] tls args: {_tls_config}")
        auth = _basic_auth_config.get_auth()
        logger.debug(f"[{self.endpoint}] auth args: {_tls_config}")
        if auth:
            request_args["auth"] = auth

        try:
            response = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
                **request_args,
            )

            was_successful = self._was_request_successful(response)

            self.result = {
                "status_code": response.status_code,
                "response": response.text,
                "success": was_successful,
            }

            if not was_successful:
                self.error = f"HTTP {response.status_code}: {response.text}"

        except requests.exceptions.Timeout:
            self.error = "Request timeout"
        except requests.exceptions.ConnectionError as e:
            self.error = f"Connection error: {str(e)}"
        except Exception as e:
            self.error = f"Unexpected error: {str(e)}"

        if self.error:
            logger.error(f"[{self.endpoint}] CFR failed with {self.error}")
            return
        logger.info(f"[{self.endpoint}] CFR success")


def print_important(important_message: str) -> None:
    logger.info(f"\n{CONSOLE_BAR}\n{important_message}\n{CONSOLE_BAR}\n")


def wait_for_port_forward_ready(process: subprocess.Popen, timeout: int = 30) -> bool:
    """
    Wait for kubectl port-forward to be ready by polling its stdout output.

    Args:
        process: The subprocess.Popen object for the port-forward command
        timeout: Maximum time to wait in seconds

    Returns:
        True if port forward is ready, False if timeout or error
    """
    start_time = time.time()

    # kubectl port-forward outputs "Forwarding from..." to stdout
    while time.time() - start_time < timeout:
        # Check if process is still running
        if process.poll() is not None:
            # Process terminated unexpectedly
            logger.error("Port forward process terminated unexpectedly")
            return False

        # Check for output, success is 'Forwarding from' failure is anything else
        readable, _, _ = select.select([process.stdout, process.stderr], [], [], 0.1)

        if process.stdout in readable:
            line = process.stdout.readline()
            if line:
                # Check for the success message
                if "Forwarding from" in line:
                    logger.debug(f"port forward is ready: {line.strip()}")
                    return True
                logger.debug(f"stdout: {line.strip()}")

        if process.stderr in readable:
            line = process.stderr.readline()
            if line:
                logger.debug(f"stderr: {line.strip()}")

        time.sleep(0.1)

    logger.error("timeout waiting for port forward to be ready")
    return False


def setup_port_forwards(
    redpanda_pods: List[str],
    namespace: str,
    starting_port: int,
    admin_port: int,
) -> List[PortForward]:
    """
    Set up kubectl port-forward for each Redpanda pod. Used to reach admin api

    Args:
        redpanda_pods: List of Redpanda pod names
        namespace: Kubernetes namespace
        starting_port: Starting local port number (default: 27900)
        admin_port: Remote port on the pod (default: 9644)

    Returns:
        List of PortForward instances for each established port forward
        All port forwards will be added to a global list for cleanup on program exit
    """
    global _active_port_forwards

    print_important("Setting up port forwarding")

    port_forwards = []
    local_port = starting_port

    for pod in redpanda_pods:
        cmd = [
            "kubectl",
            "port-forward",
            f"pod/{pod}",
            f"{local_port}:{admin_port}",
            "--namespace",
            namespace,
        ]

        logger.info(
            f"Starting port forward: localhost:{local_port} -> {pod}:{admin_port}"
        )

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            # Wait for the port forward to be ready before continuing
            if wait_for_port_forward_ready(process):
                pf = PortForward(
                    local_port=local_port, process=process, admin_port=admin_port
                )
                port_forwards.append(pf)
                _active_port_forwards.append(pf)
                logger.debug(f"Successfully established port forward for {pod}")
            else:
                err_str = f"Failed to establish port forward for {pod}"
                raise RuntimeError(err_str)

            local_port += 1
        except Exception as e:
            logger.error(f"Error starting port forward for {pod}: {str(e)}")
            raise

    logger.info(f"Successfully started all {len(port_forwards)} port forwards")

    return port_forwards


def _invoke_cfr_confirmation(cfr_params: CFRParams) -> None:
    """print the necessary info and then confirm for the last time that this is what the user wants"""
    print_important(f"Controller Forced Reconfiguration")

    node_addresses = sorted(
        cfr_params.responding_endpoints, key=lambda e: (e.ip, e.port)
    )
    dead_node_ids = sorted(list(cfr_params.dead_node_ids))

    # Final confirmation prompt with all parameters
    logger.info("CONFIRMATION REQUIRED")
    logger.info("You are about to invoke Controller Forced Reconfiguration with:")
    logger.info(
        f"  Target nodes ({len(node_addresses)}): {', '.join([f'{node.ip}:{node.port}' for node in node_addresses])}"
    )
    logger.info(f"  Dead node IDs: {dead_node_ids}")
    logger.info(f"  Surviving node count: {cfr_params.surviving_count}")
    logger.warning(
        f"\n{CONSOLE_BAR}\nThis operation may lead to data loss.\n{CONSOLE_BAR}"
    )
    _confirm_with_user_or_cancel(
        "This operation may lead to data loss.\nDo you want to proceed?"
    )


def _summarize_cfr_invocation(cfr_invokers: list[CFRInvoker]) -> None:
    print_important("Summary")

    success_count = 0
    failure_count = 0

    for invoker in cfr_invokers:
        if invoker.error:
            logger.error(f"[{invoker.endpoint}] FAILED - {invoker.error}")
            failure_count += 1
        elif invoker.result and invoker.result["success"]:
            logger.info(
                f"[{invoker.endpoint}] SUCCESS - HTTP {invoker.result['status_code']}"
            )
            success_count += 1
        else:
            logger.error(
                f"[{invoker.endpoint}] FAILED - HTTP {invoker.result['status_code']}"
            )
            failure_count += 1

    logger.info(
        f"Total: {len(cfr_invokers)} nodes | Success: {success_count} | Failed: {failure_count}"
    )

    if failure_count > 0:
        raise RuntimeError(
            f"Failed {failure_count} invocations of controller forced reconfiguration. Check if a controller leader has been reestablished. If not, the nodes may be restarted to reattempt controller forced reconfiguration."
        )
    return


def invoke_cfr_on_nodes(cfr_params: CFRParams) -> None:
    """
    Invoke Controller Forced Reconfiguration on multiple nodes concurrently.

    Args:
        cfr_params: CFRParams containing dead_node_ids, surviving_count, and responding_endpoints

    Exits with code 1 if any requests failed, 0 if all succeeded.
    """

    # print out CFR params and criteria such that the user can make an informed decision, then ask for confirmation
    _invoke_cfr_confirmation(cfr_params)

    dead_node_ids = list(cfr_params.dead_node_ids)

    invokers = [
        CFRInvoker(node, dead_node_ids, cfr_params.surviving_count)
        for node in cfr_params.responding_endpoints
    ]

    threads = []
    for invoker in invokers:
        thread = threading.Thread(target=invoker.invoke)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    _summarize_cfr_invocation(invokers)


def handle_baremetal_command(
    nodes: List[str],
    port: int,
    check_liveness: bool,
    dead_nodes: Optional[List[int]],
    surviving_count: Optional[int],
) -> None:
    """
    Handles CFR for non-Kubernetes clusters

    Args:
        nodes: List of node IP addresses
        port: Admin API port (required)
        check_liveness: Whether to perform liveness check
        dead_nodes: List of dead node IDs (required if not check_liveness)
        surviving_count: Number of surviving nodes (required if not check_liveness)
    """
    # Validate arguments
    if not check_liveness:
        if dead_nodes is None or surviving_count is None:
            raise RuntimeError(
                "Error: --dead-nodes and --surviving-count are required when not using --check-liveness"
            )

    # If check-liveness is enabled, run liveness check first
    if check_liveness:
        # Create list of RedpandaAdminEndpoint for each node
        node_addresses = [RedpandaAdminEndpoint(ip, port) for ip in nodes]
        cfr_params = perform_liveness_check_and_get_cfr_params(node_addresses)
    else:
        # Create list of RedpandaAdminEndpoint for CFR invocation
        node_addresses = [RedpandaAdminEndpoint(ip, port) for ip in nodes]

        # Construct CFRParams from provided arguments
        cfr_params = CFRParams(
            dead_node_ids=set(dead_nodes),
            surviving_count=surviving_count,
            responding_endpoints=set(node_addresses),
        )

    invoke_cfr_on_nodes(cfr_params)


def _get_pods(namespace: str) -> list[str]:
    cmd = ["kubectl", "get", "pods", "--namespace", namespace]
    logger.debug(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            logger.error(f"Error running kubectl:")
            logger.error(result.stderr)
            raise RuntimeError("kubectl command exited unsuccessfully")
    except subprocess.TimeoutExpired:
        logger.error("kubectl command timed out")
        raise
    except FileNotFoundError:
        logger.error(
            "kubectl not found. Please ensure kubectl is installed and in PATH"
        )
        raise
    except Exception as e:
        logger.error(f"Unexpected Exception: {str(e)}")
        raise

    logger.debug(result.stdout)

    def result_to_pods(result: CompletedProcess[str]) -> list[str]:
        pods = []
        lines = result.stdout.strip().split("\n")

        # Skip the header line and process each pod line
        for line in lines[1:]:
            if line.strip():
                # First column is the pod name
                pod_name = line.split()[0]
                pods.append(pod_name)
        return pods

    return result_to_pods(result)


def _filter_to_redpanda_pods(
    pods: list[str], rp_prefix: str = DEFAULT_KUBERNETES_POD_PREFIX
) -> list[str]:
    redpanda_pods = [pod for pod in pods if pod.startswith(rp_prefix)]

    logger.info(f"Found {len(redpanda_pods)} Redpanda cluster node pods:")
    for pod in redpanda_pods:
        logger.info(f"  - {pod}")
    return redpanda_pods


def _confirm_with_user_or_cancel(prompt: str) -> None:
    """asks a user whether they want to continue or exit"""
    response = input(f"{prompt} (yes/no): ").strip().lower()
    if response not in ["yes", "y"]:
        raise RuntimeError("Operation cancelled by user.")


def handle_kubernetes_command(
    namespace: str, local_port_start: int, admin_port: int
) -> None:
    """
    Handles CFR for a kubernetes cluster

    Process:
        1. uses kubectl to scrape all redpanda nodes
        2. creates a port forward to each redpanda pod's admin api
        3. uses the port forwards to follow the baremetal process (liveness check -> cfr commands)
    """
    # Run kubectl get pods
    redpanda_pods = _filter_to_redpanda_pods(_get_pods(namespace))

    newline = "\n"
    _confirm_with_user_or_cancel(
        f"Are these the right Redpanda cluster node pods?{newline}{newline.join(redpanda_pods) + newline}"
    )

    # Set up port forwarding for each pod
    port_forwards = setup_port_forwards(
        redpanda_pods=redpanda_pods,
        namespace=namespace,
        starting_port=local_port_start,
        admin_port=admin_port,
    )

    # Create RedpandaAdminEndpoint for each localhost port forward
    node_addresses = [
        RedpandaAdminEndpoint("localhost", pf.local_port) for pf in port_forwards
    ]

    # the rest of this looks exactly like baremetal cfr
    cfr_params = perform_liveness_check_and_get_cfr_params(node_addresses)

    invoke_cfr_on_nodes(cfr_params)


def perform_liveness_check_and_get_cfr_params(
    node_addresses: List[RedpandaAdminEndpoint],
) -> CFRParams:
    """
    Perform liveness check and extract CFR parameters.

    Args:
        node_addresses: List of RedpandaAdminEndpoint for each node

    Returns:
        CFRParams containing dead_node_ids, surviving_count, and responding_endpoints

    Exits if liveness check fails or consensus is not reached.
    """
    liveness_result = check_cluster_liveness(node_addresses)

    if liveness_result is None:
        logger.error("✗ Failed to create a CFR recommendation. Proceed manually.")
        raise RuntimeError("Cannot Proceed")

    # Construct CFR parameters from liveness check
    cfr_params = CFRParams(
        dead_node_ids=liveness_result.dead_nodes,
        surviving_count=len(liveness_result.alive_nodes),
        responding_endpoints=liveness_result.responding_endpoints,
    )

    print_important("Recommended Action")
    logger.info(f"Based on liveness check, the following parameters are recommended:")
    logger.info(f"  Dead node IDs: {sorted(list(cfr_params.dead_node_ids))}")
    logger.info(f"  Surviving node count: {cfr_params.surviving_count}")

    return cfr_params


def _broker_responses_to_consensus(
    broker_responses: list[LivenessCheckResult],
) -> LivenessCheckResults | None:
    if not broker_responses:
        logger.error("No nodes responded successfully. Cannot proceed.")
        return None

    # Compare alive node sets from all responding nodes
    alive_node_sets = [r.alive_nodes for r in broker_responses]
    first_alive_set = alive_node_sets[0]
    all_agree = all(alive_set == first_alive_set for alive_set in alive_node_sets)

    if not all_agree:
        logger.error(
            "Brokers do not agree on which brokers are still alive. Cannot proceed."
        )
        return None

    # if all agree its just the alive set
    alive_set = set(first_alive_set)

    # Get all node IDs from all successful responses
    all_node_ids = set()
    for response in broker_responses:
        all_node_ids.update(response.all_node_ids)

    # Does the size of the living node set match the number of responses
    living_nodes_are_reachable = len(broker_responses) == len(alive_set)
    if not living_nodes_are_reachable:
        logger.error(
            f"Brokers agree on which brokers are alive, but response number {len(broker_responses)} does not match living node count {len(alive_set)}"
        )
        return None

    return LivenessCheckResults(
        alive_nodes=alive_set,
        dead_nodes=(all_node_ids - alive_set),
        all_node_ids=all_node_ids,
        responding_endpoints=set(r.responding_endpoint for r in broker_responses),
    )


def check_cluster_liveness(
    node_addresses: List[RedpandaAdminEndpoint],
) -> LivenessCheckResults | None:
    """Perform liveness check and yield structured results"""
    print_important(f"Checking Cluster Liveness")
    logger.info(f"Querying {len(node_addresses)} nodes for broker status...")

    # Create checker instances for each node
    checkers = [LivenessChecker(node) for node in node_addresses]

    threads = []
    for checker in checkers:
        thread = threading.Thread(target=checker.check)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print_important("Liveness Check Results")

    results = [c.yield_result() for c in checkers]
    successes = [r for r in results if isinstance(r, LivenessCheckResult)]
    failures = [r for r in results if isinstance(r, LivenessCheckError)]

    logger.info(
        f"Successful Statuses: {len(successes)}/{len(checkers)}\nFailed Statuses: {len(failures)}/{len(checkers)}"
    )
    logger.debug(f"Results:\n {newline.join([str(result) for result in results])}")

    consensus = _broker_responses_to_consensus(successes)
    if not consensus:
        logger.error("No consensus, cannot proceed. Manual intervention required.")
        return None

    print_important("Consensus Reached")
    logger.info("All responding nodes agree on liveness status")
    logger.info(f"Status\n{consensus}")

    return consensus


def add_tls_arguments(parser: argparse.ArgumentParser) -> None:
    """Add TLS-related command line arguments to a parser."""
    parser.add_argument(
        "--tls-ca-cert",
        type=str,
        help="Path to CA certificate file for TLS verification (enables HTTPS)",
    )
    parser.add_argument(
        "--tls-client-cert",
        type=str,
        help="Path to client certificate file for mutual TLS authentication",
    )
    parser.add_argument(
        "--tls-client-key",
        type=str,
        help="Path to client private key file for mutual TLS authentication",
    )


def add_auth_arguments(parser: argparse.ArgumentParser) -> None:
    """Add authentication-related command line arguments to a parser."""
    parser.add_argument(
        "--username",
        type=str,
        help="Username for basic authentication (optional)",
    )
    parser.add_argument(
        "--password",
        type=str,
        help="Password for basic authentication (optional)",
    )


def add_shared_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_ADMIN_PORT,
        help=f"Admin API port (default: {DEFAULT_ADMIN_PORT})",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO"],
        help="Set the logging level (default: INFO)",
    )
    add_auth_arguments(parser)
    add_tls_arguments(parser)


def main() -> int:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter, description=description
    )

    subparsers = parser.add_subparsers(
        dest="command", help="Deployment type", required=True
    )

    # Baremetal subcommand
    baremetal_parser = subparsers.add_parser(
        "baremetal", help="Run CFR on a self managed deployment"
    )
    baremetal_parser.add_argument(
        "--nodes",
        nargs="+",
        required=True,
        help="IP addresses of the nodes in the cluster (required)",
    )
    baremetal_parser.add_argument(
        "--check-liveness",
        action="store_true",
        help="Recommend a controller forced reconfiguration command based on current node liveness reports",
    )
    baremetal_parser.add_argument(
        "--dead-nodes",
        nargs="+",
        type=int,
        help="List of dead node IDs (required if --check-liveness is not used)",
    )
    baremetal_parser.add_argument(
        "--surviving-count",
        type=int,
        help="Number of surviving nodes (required if --check-liveness is not used)",
    )
    add_shared_arguments(baremetal_parser)

    # Kubernetes subcommand
    kubernetes_parser = subparsers.add_parser(
        "kubernetes", help="Run CFR on kubernetes deployment"
    )
    kubernetes_parser.add_argument(
        "--namespace",
        type=str,
        default=DEFAULT_KUBERNETES_NAMESPACE,
        help=f"Kubernetes namespace (default: {DEFAULT_KUBERNETES_NAMESPACE})",
    )
    kubernetes_parser.add_argument(
        "--local-port-start",
        type=int,
        default=DEFAULT_LOCAL_PORT_BINDING_START,
        help=f"The first port number to use for kubernetes port forwarding, (default: {DEFAULT_LOCAL_PORT_BINDING_START}.)",
    )
    add_shared_arguments(kubernetes_parser)

    args = parser.parse_args()

    # Configure logging based on command line arguments
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(
        stream=sys.stdout,
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
    logger.debug(f"Logging level set to {args.log_level}")

    # Configure TLS settings from command line arguments
    global _tls_config
    _tls_config = TLSConfig(
        ca_cert=getattr(args, "tls_ca_cert", None),
        client_cert=getattr(args, "tls_client_cert", None),
        client_key=getattr(args, "tls_client_key", None),
    )
    logger.info(_tls_config)

    # Configure Basic Auth settings from command line arguments
    global _basic_auth_config
    _basic_auth_config = BasicAuthConfig(
        username=getattr(args, "username", None),
        password=getattr(args, "password", None),
    )
    logger.info(_basic_auth_config)

    try:
        # Handle kubernetes command
        if args.command == "kubernetes":
            handle_kubernetes_command(args.namespace, args.local_port_start, args.port)
            return 0

        # Handle baremetal command
        if args.command == "baremetal":
            handle_baremetal_command(
                args.nodes,
                args.port,
                args.check_liveness,
                args.dead_nodes,
                args.surviving_count,
            )
            return 0
        raise RuntimeError("unknown subcommand")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
