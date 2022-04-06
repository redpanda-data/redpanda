# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# This script is modeled after the llvm python script prepare-code-coverage-artifact.py
# which is used to generate coverage reports in HTML format.
# Find the original python script at https://github.com/llvm/llvm-project/blob/e356027016c6365b3d8924f54c33e2c63d931492/llvm/utils/prepare-code-coverage-artifact.py

import sys
import csv
import json
import os
import subprocess
import argparse
import tempfile


def merge_profraw_files(profraw_files, data_profile):
    llvm_profdata_merge = [
        "llvm-profdata", "merge", "-sparse", "-o", f"{data_profile.name}"
    ]
    for profraw in profraw_files:
        llvm_profdata_merge.append(profraw)

    subprocess.check_call(llvm_profdata_merge)


def check_ignore(cmd_list, ignore_regex):
    if ignore_regex:
        cmd_list.append(f"--ignore-filename-regex={ignore_regex}")


def gen_coverage_json(rp_binary, data_profile, ignore_regex):
    llvm_cov_export = [
        "llvm-cov", "export", f"{rp_binary}",
        f"-instr-profile={data_profile.name}"
    ]

    check_ignore(llvm_cov_export, ignore_regex)

    results = subprocess.run(llvm_cov_export,
                             capture_output=True,
                             encoding="utf-8")

    results = json.loads(results.stdout)
    report = []

    for f_cov in results["data"][0]["files"]:

        # Add the filename to the summary
        summary = f_cov["summary"]
        summary["filename"] = f_cov["filename"]
        report.append(summary)

    # Put the totals at the end
    totals = results["data"][0]["totals"]
    totals["filename"] = "Totals"
    report.append(totals)

    return report


def gen_coverage_html(rp_binary, data_profile, ignore_regex, out_dir):
    llvm_cov_show = [
        "llvm-cov", "show", f"{rp_binary}",
        f"-instr-profile={data_profile.name}", f"--output-dir={out_dir}",
        "-format=html", "-show-line-counts-or-regions", "-Xdemangler=c++filt"
    ]

    check_ignore(llvm_cov_show, ignore_regex)

    # The command llvm-cov will write the output
    # to the output dir
    subprocess.check_call(llvm_cov_show)


def gen_coverage_csv(report_json):
    field_names = [
        "filename", "functions.count", "functions.covered",
        "functions.percent", "lines.count", "lines.covered", "lines.percent",
        "regions.count", "regions.covered", "regions.notcovered",
        "regions.percent", "branches.count", "branches.covered",
        "branches.notcovered", "branches.percent", "instantiations.count",
        "instantiations.covered", "instantiations.percent"
    ]

    def to_csv_dict(f_cov):
        csv_dict = {}

        csv_dict["filename"] = f_cov["filename"]
        csv_dict["functions.count"] = f_cov["functions"]["count"]
        csv_dict["functions.covered"] = f_cov["functions"]["covered"]
        csv_dict["functions.percent"] = f_cov["functions"]["percent"]
        csv_dict["lines.count"] = f_cov["lines"]["count"]
        csv_dict["lines.covered"] = f_cov["lines"]["covered"]
        csv_dict["lines.percent"] = f_cov["lines"]["percent"]
        csv_dict["regions.count"] = f_cov["regions"]["count"]
        csv_dict["regions.covered"] = f_cov["regions"]["covered"]
        csv_dict["regions.notcovered"] = f_cov["regions"]["notcovered"]
        csv_dict["regions.percent"] = f_cov["regions"]["percent"]
        csv_dict["branches.count"] = f_cov["branches"]["count"]
        csv_dict["branches.covered"] = f_cov["branches"]["covered"]
        csv_dict["branches.notcovered"] = f_cov["branches"]["notcovered"]
        csv_dict["branches.percent"] = f_cov["branches"]["percent"]
        csv_dict["instantiations.count"] = f_cov["instantiations"]["count"]
        csv_dict["instantiations.covered"] = f_cov["instantiations"]["covered"]
        csv_dict["instantiations.percent"] = f_cov["instantiations"]["percent"]

        return csv_dict

    with open("coverage.csv", "w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file,
                                fieldnames=field_names,
                                delimiter=',',
                                quotechar='"',
                                quoting=csv.QUOTE_NONNUMERIC)

        writer.writeheader()

        for f_cov in report_json:
            writer.writerow(to_csv_dict(f_cov))


def main(args):
    rp_binary = os.path.join(args.build_root, "debug/clang/bin/redpanda")
    data_profile = tempfile.NamedTemporaryFile()

    # merge profraw files into the data profile
    merge_profraw_files(profraw_files=args.profraw_files,
                        data_profile=data_profile)

    if args.html:
        # get coverage report in HTML format
        gen_coverage_html(rp_binary=rp_binary,
                          data_profile=data_profile,
                          ignore_regex=args.ignore_regex,
                          out_dir=args.out_dir)

    elif args.csv:
        # First, get coverage report in JSON format
        report_json = gen_coverage_json(rp_binary=rp_binary,
                                        data_profile=data_profile,
                                        ignore_regex=args.ignore_regex)

        # convert JSON report to CSV
        gen_coverage_csv(report_json)

    else:
        # get coverage report in JSON format
        report = gen_coverage_json(rp_binary=rp_binary,
                                   data_profile=data_profile,
                                   ignore_regex=args.ignore_regex)

        with open("coverage.json", "w") as out_file:
            json.dump(report, out_file, indent=4, sort_keys=True)

    data_profile.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=
        "Given a list of .profraw files, write code coverage reports to disk")
    parser.add_argument("profraw_files",
                        metavar=".profraw",
                        type=str,
                        nargs="+",
                        help="A list of .profraw files")
    parser.add_argument("--build-root",
                        type=str,
                        required=True,
                        help="The path to redpanda/vbuild")
    parser.add_argument("--ignore-regex",
                        type=str,
                        help="Ignore files that match the regex")
    parser.add_argument("--csv",
                        action="store_true",
                        help="Enables output in CSV format")
    parser.add_argument(
        "--out-dir",
        type=str,
        help="Directory to write coverage results. Requires --html")
    parser.add_argument(
        "--html",
        action="store_true",
        help="Enables output in HTML format. Requires --out-dir")

    args = parser.parse_args()

    # Using an output directory is only necessary for
    # coverage reports in HTML format.
    # So exit if one flag is true and the other is false
    if args.html ^ bool(args.out_dir):
        print("Error: Use --html and --out-dir together")
        sys.exit(1)

    main(args)
