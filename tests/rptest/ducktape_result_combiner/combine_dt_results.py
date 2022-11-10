from ducktape.tests.reporter import SimpleStdoutSummaryReporter, SimpleFileSummaryReporter, \
    HTMLSummaryReporter, JSONReporter, JUnitReporter, SummaryReporter
from ducktape.tests.result import TestResult, TestResults
from ducktape.tests.session import SessionContext
from ducktape.tests.status import PASS, FAIL, IGNORE, OPASS, OFAIL
import json
import os
import argparse

class CustomSummaryReporter(SummaryReporter):
    def __init__(self, results, use_custom_test_log_path=False):
        SummaryReporter.__init__(self, results)
        self.use_custom_test_log_path = use_custom_test_log_path

class CustomHTMLSummaryReporter(HTMLSummaryReporter, CustomSummaryReporter):
    def test_results_dir(self, result):
        """Return *relative path* to test results directory.

        Path is relative to the base results_dir. Relative path behaves better if the results directory is copied,
        moved etc.
        """
        if self.use_custom_test_log_path:
            return result.test_log_path
        base_dir = os.path.abspath(result.session_context.results_dir)
        base_dir = os.path.join(base_dir, "")  # Ensure trailing directory indicator

        test_results_dir = os.path.abspath(result.results_dir)
        return test_results_dir[len(base_dir):]  # truncate the "absolute" portion

class CustomTestResult(TestResult):
    def __init__(self,
                 test_context,
                 test_index,
                 session_context,
                 test_status=PASS,
                 summary="",
                 data=None,
                 start_time=-1,
                 stop_time=-1,
                 load_from_json=False):
        if not load_from_json:
            TestResult.__init__(self, test_context, test_index,
                    session_context, test_status=PASS, summary="",
                    data=None, start_time=-1, stop_time=-1)
    def from_json(self, obj, sc):
        self.test_id = obj["test_id"]
        self.module_name = obj["module_name"]
        self.cls_name = obj["cls_name"]
        self.function_name = obj["function_name"]
        self.injected_args = obj["injected_args"]
        self.description = obj["description"]
        self.results_dir = obj["results_dir"]
        self.relative_results_dir = obj["relative_results_dir"]
        self.base_results_dir = obj["base_results_dir"]
        self.test_status = obj["test_status"]
        self.summary = obj["summary"]
        self.data = obj["data"]
        self.start_time = obj["start_time"]
        self.stop_time = obj["stop_time"]
        self.session_context = sc
        self.nodes_allocated = obj["nodes_allocated"]
        self.nodes_used = obj["nodes_used"]
        self.services = obj["services"]
        # custom
        self.test_log_path = obj["test_log_path"] if "test_log_path" in obj else ""
        return self

class CustomTestResults(TestResults):
    def __init__(self, session_context, cluster):
        TestResults.__init__(self, session_context, cluster)

    def from_json(self, obj, sc):
        self.ducktape_version = obj["ducktape_version"]
        self.session_context = sc
        self.start_time = obj["start_time"]
        self.stop_time = obj["stop_time"]
        self.run_time_statistics = obj["run_time_statistics"]
        self.cluster_nodes_used = obj["cluster_nodes_used"]
        self.cluster_nodes_allocated = obj["cluster_nodes_allocated"]
        self.cluster_utilization = obj["cluster_utilization"]
        self.cluster_num_nodes = obj["cluster_num_nodes"]
        self._results = []
        self.results = []
        for r in obj["results"]:
            tr = CustomTestResult(test_context="", test_index="", session_context="", load_from_json=True)
            tr = tr.from_json(r, sc)
            self._results.append(tr)
            self.results.append(tr)
        self.parallelism = obj["parallelism"]
        return self

class CustomSessionContext(SessionContext):
    def __init__(self, **kwargs):
        SessionContext.__init__(self, **kwargs)

    def from_json(self, obj):
        self.session_id = obj["session_id"]
        self.results_dir = obj["results_dir"]
        self.debug = obj["debug"]
        self.compress = obj["compress"]
        self.exit_first = obj["exit_first"]
        self.no_teardown = obj["no_teardown"]
        self.max_parallel = obj["max_parallel"]
        self.default_expected_num_nodes = obj["default_expected_num_nodes"]
        self.fail_bad_cluster_utilization = obj["fail_bad_cluster_utilization"]
        self.test_runner_timeout = obj["test_runner_timeout"]
        self._globals = obj["_globals"]
        return self


parser = argparse.ArgumentParser(description = 'ducktape test combiner')
parser.add_argument('-o', '--output', action="store", help="Output dir", required=True)
parser.add_argument('-f','--files', nargs='+', help='1 or more ducktape report.json files', required=True)
parser.add_argument('-a','--custom-test-log-path', nargs='+',
    help="Use custom test log path per file provided"
        " when clicking on `Detail` button on HTML report", required=False)

args_list = parser.parse_args()
use_custom_test_log_path = False

if args_list.custom_test_log_path:
    use_custom_test_log_path = True
    if len(args_list.custom_test_log_path) != len(args_list.files):
        print("Files and custom log path should have the same length")
        exit(1)

def get_test_results_from_json(report_json_file):
    with open(report_json_file, "r") as f:
        obj = json.loads(f.read())
    f.close()
    sc = CustomSessionContext(**obj["session_context"])
    sc = sc.from_json(obj["session_context"])
    test_results = CustomTestResults(session_context=sc, cluster=[None] * int(obj["cluster_num_nodes"]))
    test_results = test_results.from_json(obj, sc)
    return test_results

def tweak_test_log_path(results, custom_test_log_path_index, session_context):
    if not use_custom_test_log_path:
        return results
    ext_results = []
    for r in results:
        base_dir = os.path.abspath(session_context["results_dir"])
        base_dir = os.path.join(base_dir, "")  # Ensure trailing directory indicator
        test_results_dir = os.path.abspath(r["results_dir"])
        rel_path = test_results_dir[len(base_dir):] # truncate the "absolute" portion
        r["test_log_path"] = f"{args_list.custom_test_log_path[custom_test_log_path_index]}/{session_context['session_id']}/{rel_path}"
        ext_results.append(r)
    return ext_results
        
def combine_test_results(test_results, output_dir):
    obj = {}
    obj["cluster_nodes_allocated"] = {}
    obj["cluster_nodes_allocated"]["max"] = 0
    obj["cluster_nodes_allocated"]["mean"] = 0.0
    obj["cluster_nodes_allocated"]["min"] = 0
    obj["cluster_nodes_used"] = {}
    obj["cluster_nodes_used"]["max"] = 0
    obj["cluster_nodes_used"]["mean"] = 0.0
    obj["cluster_nodes_used"]["min"] = 0
    obj["cluster_num_nodes"] = 0
    obj["cluster_utilization"] = 0.0
    obj["ducktape_version"] = ""
    obj["num_failed"] = 0
    obj["num_ignored"] = 0
    obj["num_ofailed"] = 0
    obj["num_opassed"] = 0
    obj["num_passed"] = 0
    obj["parallelism"] = 0.0
    obj["results"] = []
    obj["run_time_seconds"] = 0.0
    obj["run_time_statistics"] = {}
    obj["run_time_statistics"]["max"] = 0
    obj["run_time_statistics"]["mean"] = 0.0
    obj["run_time_statistics"]["min"] = 0
    obj["session_context"] = {}
    obj["session_context"]["_globals"] = {}
    obj["session_context"]["_globals"]["enable_cov"] = ""
    obj["session_context"]["_globals"]["redpanda_log_level"] = ""
    obj["session_context"]["_globals"]["rp_install_path_root"] = ""
    obj["session_context"]["_globals"]["s3_access_key"] = ""
    obj["session_context"]["_globals"]["s3_bucket"] = ""
    obj["session_context"]["_globals"]["s3_region"] = ""
    obj["session_context"]["_globals"]["s3_secret_key"] = ""
    obj["session_context"]["_globals"]["scale"] = ""
    obj["session_context"]["_globals"]["use_xfs_partitions"] = ""
    obj["session_context"]["compress"] = ""
    obj["session_context"]["debug"] = ""
    obj["session_context"]["default_expected_num_nodes"] = ""
    obj["session_context"]["exit_first"] = ""
    obj["session_context"]["fail_bad_cluster_utilization"] = ""
    obj["session_context"]["max_parallel"] = 0
    obj["session_context"]["no_teardown"] = ""
    obj["session_context"]["results_dir"] = ""
    obj["session_context"]["session_id"] = ""
    obj["session_context"]["test_runner_timeout"] = 0
    obj["start_time"] = 0.0
    obj["stop_time"] = 0.0
    for tr in test_results:
        if int(obj["cluster_nodes_allocated"]["max"]) < int(tr["cluster_nodes_allocated"]["max"]):
            obj["cluster_nodes_allocated"]["max"] = int(tr["cluster_nodes_allocated"]["max"])
        if int(obj["cluster_nodes_allocated"]["min"]) > int(tr["cluster_nodes_allocated"]["min"]):
            obj["cluster_nodes_allocated"]["min"] = int(tr["cluster_nodes_allocated"]["min"])
        elif int(obj["cluster_nodes_allocated"]["min"]) == 0:
            obj["cluster_nodes_allocated"]["min"] = int(tr["cluster_nodes_allocated"]["min"])
        obj["cluster_nodes_allocated"]["mean"] = (obj["cluster_nodes_allocated"]["min"] + obj["cluster_nodes_allocated"]["max"])/2
        if int(obj["cluster_nodes_used"]["max"]) < int(tr["cluster_nodes_used"]["max"]):
            obj["cluster_nodes_used"]["max"] = int(tr["cluster_nodes_used"]["max"])
        if int(obj["cluster_nodes_used"]["min"]) > int(tr["cluster_nodes_used"]["min"]):
            obj["cluster_nodes_used"]["min"] = int(tr["cluster_nodes_used"]["min"])
        elif int(obj["cluster_nodes_used"]["min"]) == 0:
            obj["cluster_nodes_used"]["min"] = int(tr["cluster_nodes_used"]["min"])
        obj["cluster_nodes_used"]["mean"] = (obj["cluster_nodes_used"]["min"] + obj["cluster_nodes_used"]["max"])/2
        obj["cluster_num_nodes"] = tr["cluster_num_nodes"]
        obj["cluster_utilization"] += float(tr["cluster_utilization"])/len(test_results)
        obj["ducktape_version"] = tr["ducktape_version"]
        obj["num_failed"] += int(tr["num_failed"])
        obj["num_ignored"] += int(tr["num_ignored"])
        obj["num_ofailed"] += int(tr["num_ofailed"])
        obj["num_opassed"] += int(tr["num_opassed"])
        obj["num_passed"] += int(tr["num_passed"])
        obj["parallelism"] += float(tr["parallelism"])/len(test_results)
        ext_results = tweak_test_log_path(tr["results"], test_results.index(tr), tr["session_context"])
        obj["results"].extend(ext_results)
        obj["run_time_seconds"] += float(tr["run_time_seconds"])
        if float(obj["run_time_statistics"]["max"]) < float(tr["run_time_statistics"]["max"]):
            obj["run_time_statistics"]["max"] = float(tr["run_time_statistics"]["max"])
        if float(obj["run_time_statistics"]["min"]) > float(tr["run_time_statistics"]["min"]):
            obj["run_time_statistics"]["min"] = float(tr["run_time_statistics"]["min"])
        elif float(obj["run_time_statistics"]["min"]) == 0.0:
            obj["run_time_statistics"]["min"] = float(tr["run_time_statistics"]["min"])
        obj["run_time_statistics"]["mean"] = (obj["run_time_statistics"]["min"] + obj["run_time_statistics"]["max"])/2
        obj["session_context"]["_globals"]["enable_cov"] = tr["session_context"]["_globals"]["enable_cov"] 
        obj["session_context"]["_globals"]["redpanda_log_level"] = tr["session_context"]["_globals"]["redpanda_log_level"] 
        obj["session_context"]["_globals"]["rp_install_path_root"] = tr["session_context"]["_globals"]["rp_install_path_root"] 
        obj["session_context"]["_globals"]["s3_access_key"] = tr["session_context"]["_globals"]["s3_access_key"] 
        obj["session_context"]["_globals"]["s3_bucket"] = tr["session_context"]["_globals"]["s3_bucket"] 
        obj["session_context"]["_globals"]["s3_region"] = tr["session_context"]["_globals"]["s3_region"] 
        obj["session_context"]["_globals"]["s3_secret_key"] = tr["session_context"]["_globals"]["s3_secret_key"] 
        obj["session_context"]["_globals"]["scale"] = tr["session_context"]["_globals"]["scale"] 
        obj["session_context"]["_globals"]["use_xfs_partitions"] = tr["session_context"]["_globals"]["use_xfs_partitions"] 
        obj["session_context"]["compress"] = tr["session_context"]["compress"] 
        obj["session_context"]["debug"] = tr["session_context"]["debug"] 
        obj["session_context"]["default_expected_num_nodes"] = tr["session_context"]["default_expected_num_nodes"] 
        obj["session_context"]["exit_first"] = tr["session_context"]["exit_first"] 
        obj["session_context"]["fail_bad_cluster_utilization"] = tr["session_context"]["fail_bad_cluster_utilization"] 
        obj["session_context"]["max_parallel"] = tr["session_context"]["max_parallel"]
        obj["session_context"]["no_teardown"] = tr["session_context"]["no_teardown"] 
        obj["session_context"]["results_dir"] = output_dir
        obj["session_context"]["session_id"] = tr["session_context"]["session_id"] 
        obj["session_context"]["test_runner_timeout"] = tr["session_context"]["test_runner_timeout"]
        if float(obj["stop_time"]) < float(tr["stop_time"]):
            obj["stop_time"] = float(tr["stop_time"])
        if float(obj["start_time"]) > float(tr["start_time"]):
            obj["start_time"] = float(tr["start_time"])
        elif float(obj["start_time"]) == 0.0:
            obj["start_time"] = float(tr["start_time"])
    return obj

all_tests_results = []
for file in args_list.files:
    with open(file, "r") as f:
        report = json.loads(f.read())
    f.close()
    all_tests_results.append(report)
final_report = combine_test_results(all_tests_results, args_list.output)
with open("final_report.json", "w") as f:
    f.write(json.dumps(final_report))
f.close()

test_results = get_test_results_from_json("final_report.json")

reporters = [
    SimpleStdoutSummaryReporter(test_results),
    SimpleFileSummaryReporter(test_results),
    CustomHTMLSummaryReporter(test_results, use_custom_test_log_path),
    JSONReporter(test_results),
    JUnitReporter(test_results)
]

for r in reporters:
    r.report()
