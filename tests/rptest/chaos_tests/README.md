# Chaos tests

Ducktape port of the chaos test suite from https://github.com/redpanda-data/chaos/

These tests have a common scenario:
1. A redpanda cluster is started
2. A workload is started. Workload can be executed on one or several workers
   that write down their operations in a workload log.
3. After some warmup period a fault is introduced. Faults can be recoverable
   (they will be healed after some time) and oneoff.
4. Optionally we check if the workload made progress during the fault.
5. After some time the workload is stopped and its logs are copied to the control
   node and analyzed for consistency. Also some stats are calculated and
   throughput/latency graphs are plotted.

Original test set had the following structure:
* Each test case is a json file that specifies the scenario, the workload,
  the fault, and some additional checks and settings.
* Test cases are grouped into 8 test suites. All tests in a suite have the
  same scenario and workload but different faults.
* There are 5 scenarios and 7 workloads, i.e. some scenarios and workloads are
  reused in different workloads, but mostly it is a 1-1-1 scenario-workload-suite
  correspondence.

This is translated into the following ducktape test structure:
* Workloads are ducktape services inheriting from WorkloadServiceBase.
* Scenarios correspond to test classes. Each class inherits from the base
  scenario class, but they differ in how the prepare the cluster and topics.
* Suites correspond to test methods of the scenario classes.
* Each test case corresponds to a case_id parameter passed to a test method.
* The test method instantiates the workload and the fault (based on the
  case_id parameter) and runs the scenario.