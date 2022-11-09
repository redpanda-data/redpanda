from ducktape.tests.reporter import SimpleStdoutSummaryReporter, SimpleFileSummaryReporter, \
    HTMLSummaryReporter, JSONReporter, JUnitReporter
from ducktape.tests.result import TestResults
from ducktape.tests.session import SessionContext
import json
import os
import argparse

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
    sc = SessionContext(**obj["session_context"])
    sc = sc.from_json(obj["session_context"])
    test_results = TestResults(session_context=sc, cluster=[None] * int(obj["cluster_num_nodes"]))
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
    HTMLSummaryReporter(test_results, use_custom_test_log_path),
    JSONReporter(test_results),
    JUnitReporter(test_results)
]

for r in reporters:
    r.report()
