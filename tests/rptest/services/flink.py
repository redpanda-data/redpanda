# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import requests
import os
import yaml
import json

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service
from ducktape.utils.util import wait_until

java_opts = [
    "--add-exports=java.base/sun.net.util=ALL-UNNAMED",
    "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED",
    "--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
    "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.text=ALL-UNNAMED",
    "--add-opens=java.base/java.time=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
]

# Used to generate flink-conf.yaml
# Only at-hand parameters listed.
# Refer to original file for additional parameters.
flink_config = {
    # These parameters are required for Java 17 support.
    # They can be safely removed when using Java 8/11.
    "env.java.opts.all": " ".join(java_opts),

    # The external address of the host on which the JobManager runs and can be
    # reached by the TaskManagers and any clients which want to connect. This
    # setting is only used in Standalone mode and may be overwritten
    # on the JobManager side by specifying the --host <hostname> parameter
    # of the bin/jobmanager.sh executable.
    # In high availability mode, if you use the bin/start-cluster.sh script
    # and setup the conf/masters file, this will be taken care
    # of automatically. Yarn automatically configure the host name based
    # on the hostname of the node where the JobManager runs.
    "jobmanager.rpc.address": "localhost",

    # Python client executable
    "python.client.executable": "python3",

    # The RPC port where the JobManager is reachable.
    "jobmanager.rpc.port": 6123,

    # The host interface the JobManager will bind to. By default, this is localhost, and will prevent
    # the JobManager from communicating outside the machine/container it is running on.
    # On YARN this setting will be ignored if it is set to 'localhost', defaulting to 0.0.0.0.
    # On Kubernetes this setting will be ignored, defaulting to 0.0.0.0.
    #
    # To enable this, set the bind-host address to one that has access to an outside facing network
    # interface, such as 0.0.0.0.
    "jobmanager.bind-host": "localhost",

    # The total process memory size for the JobManager.
    # Note this accounts for all memory usage within the JobManager process, including JVM metaspace and other overhead.
    "jobmanager.memory.process.size": "1600m",

    # The host interface the TaskManager will bind to. By default, this is localhost, and will prevent
    # the TaskManager from communicating outside the machine/container it is running on.
    # On YARN this setting will be ignored if it is set to 'localhost', defaulting to 0.0.0.0.
    # On Kubernetes this setting will be ignored, defaulting to 0.0.0.0.
    #
    # To enable this, set the bind-host address to one that has access to an outside facing network
    # interface, such as 0.0.0.0.
    "taskmanager.bind-host": "localhost",

    # The address of the host on which the TaskManager runs and can be reached by the JobManager and
    # other TaskManagers. If not specified, the TaskManager will try different strategies to identify
    # the address.
    #
    # Note this address needs to be reachable by the JobManager and forward traffic to one of
    # the interfaces the TaskManager is bound to (see 'taskmanager.bind-host').
    #
    # Note also that unless all TaskManagers are running on the same machine, this address needs to be
    # configured separately for each TaskManager.
    "taskmanager.host": "localhost",

    # The total process memory size for the TaskManager.
    #
    # Note this accounts for all memory usage within the TaskManager process, including JVM metaspace and other overhead.
    "taskmanager.memory.process.size": "1728m",

    # To exclude JVM metaspace and overhead, please, use total Flink memory size instead of 'taskmanager.memory.process.size'.
    # It is not recommended to set both 'taskmanager.memory.process.size' and Flink memory.
    #
    "taskmanager.memory.flink.size": "1280m",

    # The number of task slots that each TaskManager offers. Each slot runs one parallel pipeline.
    "taskmanager.numberOfTaskSlots": 1,

    # The parallelism used for programs that did not specify and other parallelism.
    "parallelism.default": 1,

    # Enable detailed metrics for network subsystem
    "taskmanager.network.detailed-metrics": True,

    # ==============================================================================
    # Rest & web frontend
    # ==============================================================================

    # The port to which the REST client connects to. If rest.bind-port has
    # not been specified, then the server will bind to this port as well.
    #
    "rest.port": 8090,

    # The address to which the REST client will connect to
    "rest.address": "localhost",

    # Port range for the REST and web server to bind to.
    # rest.bind-port: 8080-8090

    # The address that the REST & web server binds to
    # By default, this is localhost, which prevents the REST & web server from
    # being able to communicate outside of the machine/container it is running on.
    #
    # To enable this, set the bind address to one that has access to outside-facing
    # network interface, such as 0.0.0.0.
    "rest.bind-address": "0.0.0.0",

    # Flag to specify whether job submission is enabled from the web-based
    # runtime monitor. Uncomment to disable.
    "web.submit.enable": False,

    # Flag to specify whether job cancellation is enabled from the web-based
    # runtime monitor. Uncomment to disable.
    "web.cancel.enable": False,

    # Metrics
    "metrics.recording.level": "debug"
}


class FlinkService(Service):
    """
    Service that runs Job and Task managers on single node
    Jobs are separate python scripts at this point.
    """
    FLINK_HOME = "/opt/flink/"
    FLINK_LOGS = FLINK_HOME + "log"
    FLINK_BIN = FLINK_HOME + "bin/"
    FLINK_CONFIG_FILE_PATH = FLINK_HOME + "conf/flink-conf.yaml"
    FLINK_START = FLINK_BIN + "start-cluster.sh"
    FLINK_STOP = FLINK_BIN + "stop-cluster.sh"
    FLINK_TASKMAN_START = FLINK_BIN + "taskmanager.sh start"
    FLINK_TASKMAN_STOP_ALL = FLINK_BIN + "taskmanager.sh stop-all"
    FLINK_WORKLOADS_FOLDER = "/workloads"
    FLINK_WORKLOAD_CONFIG_FILENAME = "flink_workload_config.json"

    STATE_INIT = 'INITIALIZING'
    STATE_RECONCILING = 'RECONCILING'
    STATE_RUNNING = 'RUNNING'
    STATE_CANCELING = 'CANCELING'
    STATE_DEPLOYING = 'DEPLOYING'
    STATE_RESTARTING = 'RESTARTING'

    STATE_CREATED = 'CREATED'
    STATE_SCHEDULED = 'SCHEDULED'
    STATE_FAILED = 'FAILED'
    STATE_FINISHED = 'FINISHED'
    STATE_CANCELED = 'CANCELED'

    def __init__(self, context, num_cpus, node_ram_mb, *args, **kwargs):
        # No custom node support at this time
        nodes_to_allocate = 1
        # Init service
        super(FlinkService, self).__init__(context,
                                           num_nodes=nodes_to_allocate,
                                           *args,
                                           **kwargs)

        self.flink_rest_port = flink_config["rest.port"]
        # Map statuses
        self.job_active_statuses = [
            self.STATE_INIT, self.STATE_RECONCILING, self.STATE_RUNNING,
            self.STATE_CANCELING, self.STATE_DEPLOYING, self.STATE_RESTARTING,
            self.STATE_CREATED, self.STATE_SCHEDULED
        ]
        self.job_inactive_statuses = [
            self.STATE_FAILED, self.STATE_FINISHED, self.STATE_CANCELED
        ]

        self.logger.info(f"Using specs: {num_cpus} cpus, {node_ram_mb}MB RAM")
        self.service_config = flink_config

        tm_count = num_cpus
        if node_ram_mb < 10240:
            # Handle service configuration for single node in case docker
            # I.e. low memory scenarios
            # 0% ram - no ram reserve needed for docker virtual memory
            # 792mb ram - job manager
            # total - 792mb = 1384mb ram - for taskmanager
            # Tight memory scenario
            jm_ram = 792
            tm_ram = int(node_ram_mb - jm_ram // num_cpus)
            if tm_ram < 1024:
                tm_count = 1
                new_tm_ram = node_ram_mb - jm_ram
                self.logger.info(f"Using {tm_count} taskmanager with "
                                 f"{new_tm_ram}MB due to low calculated "
                                 f"memory size ({tm_ram}MB)")
                tm_ram = new_tm_ram
            # use min memory, default case is 792mb out of 2048mb
            self.service_config[
                'jobmanager.memory.process.size'] = f"{jm_ram}m"
            # default, ~1280mb out of 2048 if 1 cpu
            self.service_config[
                'taskmanager.memory.process.size'] = f"{tm_ram}m"
            # 1000mb, total - 792mb for JVM metadata, etc
            self.service_config[
                'taskmanager.memory.flink.size'] = f"{tm_ram - 512}m"
        else:
            # Handle service configuration for single node in case of EC2
            # 10% ram - reserved
            # 10% ram - job manager
            # 80% ram / vcpus - per taskmanager
            # Example, xlarge, 32 ram, 4 vcpus
            # 32 * 0.1 = 3 (rounded down)
            # 32 * 0.8 / vcpus = 6 (rounded down)
            node_ram_gb = node_ram_mb // 1024
            jm_ram = int(node_ram_gb * 0.1)
            tm_ram = int(node_ram_gb * 0.8 // num_cpus)
            # default, 1600m
            self.service_config[
                'jobmanager.memory.process.size'] = f"{jm_ram}g"
            # default, 1728m
            self.service_config[
                'taskmanager.memory.process.size'] = f"{tm_ram}g"
            # 1280m, total - 1g for JVM metadata, etc
            self.service_config[
                'taskmanager.memory.flink.size'] = f"{tm_ram-1}g"

        # Task slots is half cpu cores, 1
        self.service_config['taskmanager.numberOfTaskSlots'] = 1
        # Set default per-task parallelism to number of vcpus
        self.service_config['pipeline.max-parallelism'] = tm_count
        # Set maximum default paralel tasks to 2xvcpus
        self.service_config[
            'execution.batch.adaptive.auto-parallelism.max-parallelism'] = \
            tm_count * 2
        # Adaptive scheduler that handles parallelism auto configuration
        self.service_config['jobmanager.scheduler'] = 'Adaptive'
        # Task restarts with delay of 30 s if rate per interval exceeded 5
        # Restarts needed due to discunnects or overloads
        # Ref: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/task_failure_recovery/#failure-rate-restart-strategy
        self.service_config['restart-strategy.type'] = 'failure-rate'
        # Delay between restarts
        self.service_config['restart-strategy.failure-rate.delay'] = '30s'
        # Rate measuring interval
        self.service_config[
            'restart-strategy.failure-rate.failure-rate-interval'] = '10s'
        # Failures per interval
        self.service_config[
            'restart-strategy.failure-rate.max-failures-per-interval'] = 50

        self.logger.info("Flink service configufation is:\n"
                         f"{json.dumps(self.service_config, indent=2)}")
        # No need to further tinker with parallelism settings, it is adaptive

        # It is better to run multiple task managers. So, there should be
        # one task manager running for each vcpus. This will provide
        # efficient use of resources
        self.num_taskmanagers = tm_count
        # internal metric storage
        self._metric = {}
        self.job_idle_threshold_ms = 30000

        # Safe var announce for log handling
        self.node = None
        self.hostname = None

    def run_flink_job(self, workload_path, workload_config, detached=True):
        """
            Runs a job on flink using current redpanda broker address.
            Not using REST for this to eliminate expensive and
            complex file upload routine via milti-part MIME

            workload_path: path to jar or py file
            detach: detaches after adding the job
        """
        # Check that supplied path exists
        if not os.path.exists(workload_path):
            raise RuntimeError("Workload not found in path "
                               f"'{workload_path}'")

        # Check for the folder and create
        n = self.nodes[0]
        self.logger.debug("Checking and creating folder on target node "
                          f"'{self.FLINK_WORKLOADS_FOLDER}'")
        if not n.account.exists(self.FLINK_WORKLOADS_FOLDER):
            n.account.mkdir(self.FLINK_WORKLOADS_FOLDER)

        # Copy workload to node
        self.logger.debug(f"Copy workload to flink node: {workload_path}")
        n.account.copy_to(workload_path, self.FLINK_WORKLOADS_FOLDER)

        # Create workload config
        config_path = os.path.join(self.FLINK_WORKLOADS_FOLDER,
                                   self.FLINK_WORKLOAD_CONFIG_FILENAME)
        n.account.create_file(config_path, json.dumps(workload_config))

        # Extract the workload script filename
        script = os.path.split(workload_path)[-1]

        # Determine active python location
        python3_path = n.account.ssh_output("which python3") \
            .decode() \
            .strip()

        # Start job
        run_path = os.path.join(self.FLINK_WORKLOADS_FOLDER, script)
        cmd = f"sudo {self.FLINK_BIN}flink run"
        if script.endswith(".jar"):
            cmd += f" {run_path}"
        elif script.endswith(".py"):
            cmd += f" -pyclientexec {python3_path}"
            cmd += f" -pyexec {python3_path}"
            cmd += f" -py {run_path}"

        if detached:
            cmd += " -d"

        self.logger.debug(f"Running flink job: {cmd}")
        try:
            out = n.account.ssh_output(cmd)
            out = out.decode()
        except RemoteCommandError as re:
            self.logger.error(f"Failed to run job for {workload_path}:\n{re}")
            return None

        self.logger.debug(f"Run job returned:\n{out}")
        generated_ids = []
        for line in out.splitlines():
            # Example of what needs to be parsed
            # Job has been submitted with JobID 8d29c8ab8e6e6634875a1cd16e879802
            idx = line.find("JobID")
            if idx > 0:
                id = line[idx + 6:]
                self.logger.debug(f"Found JobID:\n{id}")
                generated_ids.append(id)

        return generated_ids

    def _get(self, node, rest_handle):
        """
            Perform a GET to Flink REST API
            More here:
            https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/rest_api/
        """
        self.flink_baseurl = \
            f"http://{node.account.hostname}:{self.flink_rest_port}"

        url = f"{self.flink_baseurl}{rest_handle}"
        try:
            r = requests.get(url)
        except Exception as e:
            raise RuntimeError(f"Failed to get data from '{url}'") from e
        return r.json()

    def list_jobs(self, node):
        """
            List all jobs
        """
        handle = "/jobs"
        return self._get(node, handle)

    def get_job_by_id(self, jobid):
        """
            Gets job specs
        """
        handle = f"/jobs/{jobid}"
        if self.node is None:
            return {}
        else:
            return self._get(self.node, handle)

    def _run_cmd(self, node, cmd):
        out = node.account.ssh_output(cmd)
        self.logger.info(f"Response:\n{out.decode()}")

    def start_node(self, node):
        # Prepare config
        # Dump yaml and prevent java options to be divided
        conf = yaml.dump(self.service_config, width=float("inf"))
        self.logger.info("Creating flink configuration file")
        node.account.create_file(self.FLINK_CONFIG_FILE_PATH, conf)

        # Start
        self.logger.info("Starting Flink standalone cluster")
        self._run_cmd(node, self.FLINK_START)
        self.num_active_taskmanagers = 1

        # Run additional task managers
        self.logger.info("Starting additional task managers")
        while self.num_active_taskmanagers < self.num_taskmanagers:
            self.logger.info(f"Task manager {self.num_active_taskmanagers+1} "
                             "is starting")
            self._run_cmd(node, self.FLINK_TASKMAN_START)
            self.num_active_taskmanagers += 1

        # Save node data for future log handling
        self.node = node
        # Docker compatibility
        # While in EC2 hostname and actual hostname is the same,
        # in docker hostname is the container hash.
        self.hostname = node.account.ssh_output("hostname").decode().strip()
        return

    def stop_node(self, node):
        # Stop task managers
        self.logger.info("Stopping flink task managers")
        self._run_cmd(node, self.FLINK_TASKMAN_STOP_ALL)

        # Stop cluster
        self.logger.info("Stopping flink standalone cluster")
        self._run_cmd(node, self.FLINK_STOP)

        # No local node or hostname removing as they needed for log extraction
        return

    def get_active_jobs(self, node):
        """
            Get all active jobs from flink REST API
        """
        jobs = self.list_jobs(node)
        active = [
            job for job in jobs['jobs']
            if job['status'] in self.job_active_statuses
        ]
        return active

    def is_job_idle(self, jobid) -> bool:
        """
            Gets job data and ensures that it is idle for job_idle_threshold_ms
            Each job vertice should have not less than idle_threshold value
            as recorded and available in
              <rest_api>/jobs/<jobid>/vertices/*/metrics/accumulated-idle-time
            By default, 60 sec min of accumulated idle time since last non-zero 
            busy time

            jobid: as returned by <rest_api>/jobs
        """
        # If flink is not running then the job sure is idle :)
        if self.node is None:
            return True
        # Get job details
        job_details = self._get(self.node, f"/jobs/{jobid}")
        self.logger.debug(f"Checking if job '{job_details['name']}' "
                          f"({job_details['jid']}) is idle")
        # get metrics for all job vertices
        vertices = [v for v in job_details['vertices']]
        # Check each vertice
        idle_vertices = []
        for v in vertices:
            metric_key = f"{job_details['jid']}_{v['id']}_" \
                "last_idle_time_when_busy"
            # Shorthands
            idle_time = v['metrics']['accumulated-idle-time']
            busy_time = v['metrics']['accumulated-busy-time']
            # save current value for safety if this is the first time
            if metric_key not in self._metric:
                self._metric[metric_key] = idle_time
            # Prepare debug message
            log_msg = f"Vertice '{v['id']}': " \
                      f"'accumulated-idle-time' = {idle_time} ms, " \
                      f"'accumulated-busy-time' = {busy_time} ms -> "

            # If job is busy, save idle time
            if busy_time == 'NaN' or busy_time > 0:
                self._metric[metric_key] = idle_time
                # vertice is busy
                idle_vertices.append(False)
                log_msg += "is BUSY"
            else:
                # calculate idle time since last 'busy' status
                idle_since_busy = idle_time - self._metric[metric_key]
                if idle_since_busy > self.job_idle_threshold_ms:
                    # vertice is idle
                    idle_vertices.append(True)
                    log_msg += f"is IDLE (idle for {idle_since_busy} ms)"
                else:
                    idle_vertices.append(False)
                    log_msg += f"is IDLE (time to threshold " \
                        f"{self.job_idle_threshold_ms - idle_since_busy} ms)"
            # log message
            self.logger.debug(log_msg)
        # All vertices should be idle/True
        return all(idle_vertices)

    def _has_active_jobs(self, node, detect_idle_jobs=True):
        """
            Get active job list, log aggregated status count
            Return Bool valus if active jobs >0
        """
        # Get jobs in 'RUNNING' status
        jobs = self.get_active_jobs(node)
        # Log job status map
        map = {}
        for j in jobs:
            s = j['status']
            if s not in map:
                map[s] = 1
            else:
                map[s] += 1
        out = [f"{k}: {v}" for k, v in map.items()]
        if len(out) > 0:
            self.logger.debug(f"Flink active jobs; {', '.join(out)}")
        else:
            self.logger.debug("Flink has no active jobs")

        # Build list of idle jobs
        active_jobs = [j['id'] for j in jobs]
        if detect_idle_jobs:
            # generate list of jobs that can be consudered as idle
            idle_jobs = [
                job['id'] for job in jobs if self.is_job_idle(job['id'])
            ]
            if len(idle_jobs) > 0:
                self.logger.info(f"Detected idle jobs: {', '.join(idle_jobs)}")
                # subtract them from active
                active_jobs = list(set(active_jobs) - set(idle_jobs))
        # return True if >0
        return len(active_jobs) > 0

    def wait_node(self, node, timeout_sec=300, detect_idle_jobs=True):
        """
            Wait for all jobs to finish, default timeout is half an hour
        """
        # Flush internal metrics
        self._metric = {}
        wait_until(lambda: not self._has_active_jobs(
            node, detect_idle_jobs=detect_idle_jobs),
                   timeout_sec=timeout_sec,
                   backoff_sec=5)

        return True

    def clean_node(self, node):
        # Cleanup any redundant logs only if there is no service running.
        # I.e. if self.node if None, it means that this is called from 'start'
        # method and cleaning is relevant
        if self.node is None:
            node.account.ssh_output(f"sudo rm -f {self.FLINK_LOGS}/*")
        else:
            # Clear local vars
            self.node = None
            self.hostname = None
            self._metric = {}

            # Clean workloads folder
            node.account.ssh_output(
                f"sudo rm -rf {self.FLINK_WORKLOADS_FOLDER}")

        return

    @property
    def logs(self):
        # Safeguard if service not created
        if self.node is None or self.hostname is None:
            return {}
        # This will be run for all calls to logs property
        # Caching is not used for simplicity
        # Load all logs in folder
        files = self.nodes[0].account.ssh_output(
            f"ls -1 {self.FLINK_LOGS}").decode()
        # Filter out only ones from this node
        # Collect only those from this node as the flink service binded to
        # localhost and no requests or other service should be able to
        # access it from any other node
        # TODO: Update this in case of creating HA version
        logfiles = [fn for fn in files.splitlines() if self.hostname in fn]
        # Build log map for ducktape copy
        # There might be multiple log files for the same keyword and this is
        # the reason index is used. Goal is to enumerate all that is generated
        # Include any workload configuratin jsons
        log_map = {
            "workload_cfg": {
                "path":
                os.path.join(self.FLINK_WORKLOADS_FOLDER,
                             self.FLINK_WORKLOAD_CONFIG_FILENAME),
                "collect_default":
                True
            }
        }
        # all files will look like this:
        # flink-root-<keyword>-<internal_index>-<source-hostname>.<log|out>
        keywords = ["client", "standalonesession", "taskexecutor"]
        for keyword in keywords:
            targetfiles = [
                logfile for logfile in logfiles if keyword in logfile
            ]
            for idx in range(len(targetfiles)):
                targetfile = targetfiles[idx]
                # Extract extension
                ext = targetfile[targetfile.rindex('.') + 1:]
                # Add log item
                log_map.update({
                    f"{self.who_am_i().lower()}_{keyword}_{idx}_{ext}": {
                        "path": f"{self.FLINK_LOGS}/{targetfile}",
                        "collect_default": True
                    }
                })
        return log_map
