import sys
import json
import os
import subprocess
import io
import argparse
import tempfile
import gen_coverage


class DucktapeResults:
    def __init__(self, base_results_dir, current_results_dir):
        self.base_results_dir = base_results_dir
        self.current_results_dir = current_results_dir
        self.report = 'report.json'
        self.differences = {
            # The tests that did not run.
            # These tests are within base_results_dir but
            # are not within current_results_dir.
            'removed': [],

            # The new tests.
            # These tests are within current_results_dir
            # but are not within base_results_dir.
            'created': [],

            # The tests shared between base_results_dir
            # and current_results_dir. These tests
            # may have differences between them.
            'shared': []
        }

    def create_test_dict(self, test):
        test_dict = {}
        test_dict['module_name'] = test['module_name']
        test_dict['cls_name'] = test['cls_name']
        test_dict['function_name'] = test['function_name']
        test_dict['test_status'] = test['test_status']
        test_dict['run_time_seconds'] = test['run_time_seconds']

        return test_dict

    def sub_test_dicts(self, base_test, current_test):
        test_dict = self.create_test_dict(base_test)

        test_dict['test_status'] = {
            'base': base_test['test_status'],
            'curr': current_test['test_status']
        }

        test_dict['run_time_seconds'] = {
            'base':
            base_test['run_time_seconds'],
            'curr':
            current_test['run_time_seconds'],
            'diff':
            abs(base_test['run_time_seconds'] -
                current_test['run_time_seconds'])
        }

        return test_dict

    def summarize(self, report_file_path):
        summary = {
            'general': {
                'total_tests': 0,
                'status_distribution': [],
                'total_run_time_seconds': 0.0
            },
            'tests': []
        }

        with open(report_file_path) as report_file:
            report = json.load(report_file)

            summary['general']['status_distribution'] = [
                report['num_passed'], report['num_failed'],
                report['num_ignored']
            ]

            summary['general']['total_tests'] = sum(
                summary['general']['status_distribution'])

            for test in report['results']:
                test_dict = self.create_test_dict(test)
                summary['general']['total_run_time_seconds'] += test[
                    'run_time_seconds']

                summary['tests'].append(test_dict)

        return summary

    def inside(self, test, test_list):
        for test_ in test_list:
            if test['cls_name'] == test_['cls_name'] and test[
                    'function_name'] == test_['function_name']:
                return test_

        return None

    def compare_and_contrast(self, base_tests, current_tests):
        for base_test in base_tests:
            curr_test = self.inside(base_test, current_tests)

            if curr_test:
                diff_test = self.sub_test_dicts(base_test, curr_test)
                self.differences['shared'].append(diff_test)
            else:
                self.differences['removed'].append(base_test)

        for curr_test in current_tests:
            base_test = self.inside(curr_test, base_tests)

            if not base_test:
                self.differences['created'].append(curr_test)

        return self.differences

    def calculate_diffs(self):
        base_results_summary = self.summarize(
            os.path.join(self.base_results_dir, self.report))
        current_results_summary = self.summarize(
            os.path.join(self.current_results_dir, self.report))

        return self.compare_and_contrast(base_results_summary['tests'],
                                         current_results_summary['tests'])


class CoverageResults:
    def __init__(self, profraw_files, rp_binary, filter_target):
        self.profraw_files = profraw_files
        self.filter_target = filter_target
        self.rp_binary = rp_binary

    def get_code_coverage(self):
        reports = {}

        for test_func in self.profraw_files:
            cov_agent = gen_coverage.CoverageAgent(
                self.profraw_files[test_func], self.rp_binary,
                self.filter_target)

            # merge profraw files
            cov_agent.merge_profraw_files()

            # get coverage report
            reports[test_func] = cov_agent.get_coverage_report()

        return reports


def report_ducktape(differences, enable_json=False):
    if not enable_json:
        print('# Differences between shared tests\n')
        for test in differences['shared']:
            print(f'Module: {test["module_name"]}')
            print(f'Class: {test["cls_name"]}')
            print(f'Function: {test["function_name"]}')
            success_run = test["test_status"]["base"]
            current_run = test["test_status"]["curr"]
            print(
                f'Result: [ Success Run: {success_run}, Current Run: {current_run} ]'
            )
            success_run = test["run_time_seconds"]["base"]
            current_run = test["run_time_seconds"]["curr"]
            diff = test["run_time_seconds"]["diff"]
            print(
                'Runtime (seconds): [ Success Run: %.3f, Current Run: %.3f, Diff: %.3f ]\n'
                % (success_run, current_run, diff))

        print('\n# Removed tests\n')
        for test in differences['removed']:
            print(f'Module: {test["module_name"]}')
            print(f'Class: {test["cls_name"]}')
            print(f'Function: {test["function_name"]}')
            print(f'Result: {test["test_status"]}')
            print('Runtime (seconds): %.3f\n' % test["run_time_seconds"])

        print('\n# Created tests\n')
        for test in differences['created']:
            print(f'Module: {test["module_name"]}')
            print(f'Class: {test["cls_name"]}')
            print(f'Function: {test["function_name"]}')
            print(f'Result: {test["test_status"]}')
            print('Runtime (seconds): %.3f\n' % test["run_time_seconds"])
    else:
        print(json.dumps(differences, indent=4, sort_keys=True))


def report_coverage(coverage, enable_json=False):
    if not enable_json:
        print('# Client code coverage\n')
        for line in coverage:
            print(line)
    else:
        print(json.dumps(coverage, indent=4, sort_keys=True))


def create_profraw_files_dict(files_list):
    profraw_files = {}

    for profraw in files_list:
        sub_dirs = profraw.split("/")
        test_func = sub_dirs[-5]

        if test_func not in profraw_files:
            profraw_files[test_func] = []

        profraw_files[test_func].append(profraw)

    return profraw_files


def get_profraw_files(test_dir):
    # need shell=True for wildcard use
    find = f"find \"{test_dir}\" -name \"*.profraw\""
    results = subprocess.run(find,
                             shell=True,
                             capture_output=True,
                             encoding="utf-8")

    results = results.stdout.strip().split("\n")
    return create_profraw_files_dict(results)


def main(args):

    base_results_dir = os.path.join(args.build_root, "ducktape/results",
                                    args.base_results_dir)
    current_results_dir = os.path.join(args.build_root, "ducktape/results",
                                       args.current_results_dir)
    profraw_files = get_profraw_files(current_results_dir)

    # cannot use dist/local/redpanda/bin/redpanda because
    # llvm-cov says it's an invalid binary
    rp_binary = os.path.join(args.build_root, "debug/clang/bin/redpanda")

    ducktape = DucktapeResults(base_results_dir=base_results_dir,
                               current_results_dir=current_results_dir)
    ducktape_differences = ducktape.calculate_diffs()
    report_ducktape(ducktape_differences, args.json)

    print('\n')

    coverage = CoverageResults(profraw_files=profraw_files,
                               rp_binary=rp_binary,
                               filter_target=args.target)
    client_coverage = coverage.get_code_coverage()
    report_coverage(client_coverage, args.json)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Summarize the last ducktape test")
    parser.add_argument("--build-root",
                        type=str,
                        default="redpanda/vbuild",
                        help="the path to redpanda/vbuild")
    parser.add_argument("--base-results-dir",
                        type=str,
                        required=True,
                        help="the dir of the last successful ducktape run")
    parser.add_argument("--current-results-dir",
                        type=str,
                        required=True,
                        help="the dir of the latest ducktape run")
    parser.add_argument("--target",
                        type=str,
                        default="src/v",
                        help="get coverage for files in this path")
    parser.add_argument("--json",
                        action="store_true",
                        help="enables output in json format")

    args = parser.parse_args()

    main(args)
