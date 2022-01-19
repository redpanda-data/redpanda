import sys
import csv
import json
import os
import subprocess
import io
import argparse
import tempfile
import gen_coverage as rpcov

KCLIENTS = ["FranzGo", "KafkaStreams", "Sarama"]


def create_profraw_files_dict(files_list):
    profraw_files = {}

    for profraw in files_list:
        sub_dirs = profraw.split("/")
        # The path to the ducktape test will also serve
        # as the key for the test's profraw files
        duck_test = os.path.join("/", *sub_dirs[:-3])

        if duck_test not in profraw_files:
            profraw_files[duck_test] = []

        profraw_files[duck_test].append(profraw)

    return profraw_files


def get_profraw_files(test_dir):
    # need shell=True for wildcard use
    find = f'find "{test_dir}" -name "*.profraw"'
    results = subprocess.run(find,
                             shell=True,
                             capture_output=True,
                             encoding="utf-8")

    results = results.stdout.strip().split("\n")
    return create_profraw_files_dict(results)


def gen_coverage(profraw_files, rp_binary, ignore_regex):
    cov_totals = {}

    for duck_test in profraw_files:
        data_profile = tempfile.NamedTemporaryFile()

        rpcov.merge_profraw_files(profraw_files=profraw_files[duck_test],
                                  data_profile=data_profile)
        rpcov.gen_coverage_html(rp_binary=rp_binary,
                                data_profile=data_profile,
                                ignore_regex=ignore_regex,
                                out_dir=duck_test)
        cov_json = rpcov.gen_coverage_json(rp_binary=rp_binary,
                                           data_profile=data_profile,
                                           ignore_regex=ignore_regex)

        # Writes coverage.json for each test
        cov_path = os.path.join(duck_test, "coverage.json")
        with open(cov_path, "w") as out_file:
            json.dump(cov_json, out_file, indent=4, sort_keys=True)

        # The last index has the totals for the test case
        cov_totals[duck_test] = cov_json[-1]

        data_profile.close()

    return cov_totals


def check_compat_tests(test_dir):
    report_json = None
    report_path = os.path.join(test_dir, "report.json")
    with open(report_path, "r") as json_file:
        report_json = json.load(json_file)

    compat_results = {}
    for kclient in KCLIENTS:
        kclient_tests = list(
            filter(lambda test: kclient in test["test_id"],
                   report_json["results"]))

        num_pass = 0
        total = len(kclient_tests)
        for duck_test in kclient_tests:
            num_pass += duck_test["test_status"] == "PASS"

        compat_results[kclient] = [num_pass, total]

    return compat_results


def create_dashboard_page(duck_sess, dash_path, cov_totals, compat_results):
    html_template = """
<!DOCTYPE html>
<html>
<head>
<style>
table {
  border-collapse: collapse;
  width: 100%;
}

td, th {
  border: 1px solid #dddddd;
  text-align: left;
  padding: 0.2%;
}

tr:nth-child(even) {
  background-color: #dddddd;
}
</style>
</head>
<body>
    """

    # add dashboard header and coverage totals
    html_template += f"""
<h1>Coverage Dashboard</h1>
<h2>Ducktape Session: {duck_sess}</h2>
<h3>Coverage Totals</h3>
<table>
  <tr>
    <th>File</th>
    <th>Function</th>
    <th>Line</th>
    <th>Region</th>
    <th>Branch</th>
  </tr>
    """

    for duck_test in cov_totals:
        f_covered = cov_totals[duck_test]["functions"]["covered"]
        f_count = cov_totals[duck_test]["functions"]["count"]
        f_percent = cov_totals[duck_test]["functions"]["percent"]
        l_covered = cov_totals[duck_test]["lines"]["covered"]
        l_count = cov_totals[duck_test]["lines"]["count"]
        l_percent = cov_totals[duck_test]["lines"]["percent"]
        r_covered = cov_totals[duck_test]["regions"]["covered"]
        r_count = cov_totals[duck_test]["regions"]["count"]
        r_percent = cov_totals[duck_test]["regions"]["percent"]
        b_covered = cov_totals[duck_test]["branches"]["covered"]
        b_count = cov_totals[duck_test]["branches"]["count"]
        b_percent = cov_totals[duck_test]["branches"]["percent"]

        f_cov = f"{f_percent:.2f}% ({f_covered}/{f_count})"
        l_cov = f"{l_percent:.2f}% ({l_covered}/{l_count})"
        r_cov = f"{r_percent:.2f}% ({r_covered}/{r_count})"
        b_cov = f"{b_percent:.2f}% ({b_covered}/{b_count})"

        sub_dirs = duck_test.split("/")
        test_signature = f"{sub_dirs[-3]}.{sub_dirs[-2]}"

        html_template += f"""
  <tr>
    <td><a href="{duck_test}/">{test_signature}</a></td>
    <td>{f_cov}</td>
    <td>{l_cov}</td>
    <td>{r_cov}</td>
    <td>{b_cov}</td>
  </tr>
        """

    html_template += f"""
</table>
<hr>
<h3>Compatibility Results per Kafka Client</h3>
<table>
  <tr>
    <th>Kafka Client</th>
    <th>Passes/Total</th>
  </tr>
    """
    for kclient in compat_results:
        num_pass = compat_results[kclient][0]
        total = compat_results[kclient][1]

        html_template += f"""
  <tr>
    <td>{kclient}</td>
    <td>{num_pass}/{total}</td>
  </tr>
        """

    html_template += """
</table>
</body>
</html>
    """

    with open(dash_path, "w") as dash_page:
        dash_page.write(html_template)


def main(args):
    duck_sess = os.path.join(args.build_root, "ducktape/results",
                             args.ducktape_session)

    print("Getting profraw files ...")
    profraw_files = get_profraw_files(test_dir=duck_sess)
    rp_binary = os.path.join(args.build_root, "debug/clang/bin/redpanda")

    # generate code coverage report for each ducktape test
    # and capture the totals
    print("Generating coverage reports ...")
    cov_totals = gen_coverage(profraw_files=profraw_files,
                              rp_binary=rp_binary,
                              ignore_regex=args.coverage_ignore_regex)

    # check test status for the Kafka Clients we do compat testing on
    print("Checking status of compat tests ...")
    compat_results = check_compat_tests(test_dir=duck_sess)

    # write coverage dash html file
    print("Writing coverage dashboard html ...")
    dash_path = os.path.join(duck_sess, "coverage_dash.html")
    create_dashboard_page(duck_sess=args.ducktape_session,
                          dash_path=dash_path,
                          cov_totals=cov_totals,
                          compat_results=compat_results)

    print("... Done.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Summarize the last ducktape test")
    parser.add_argument("--build-root",
                        type=str,
                        required=True,
                        help="the path to redpanda/vbuild")
    parser.add_argument("--ducktape-session",
                        type=str,
                        required=True,
                        help="the dir of the ducktape session")
    parser.add_argument(
        "--coverage-ignore-regex",
        type=str,
        help="When calculating code coverage, ignore files that match the regex"
    )

    args = parser.parse_args()

    main(args)
