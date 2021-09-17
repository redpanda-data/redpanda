import sys
import json
import os
import subprocess
import io
import argparse
import tempfile


class CoverageAgent:
    def __init__(self, profraw_files, rp_binary, filter_target):
        self.profraw_files = profraw_files
        self.rp_binary = rp_binary
        self.output_file = tempfile.NamedTemporaryFile()
        self.filter_target = filter_target

    def create_coverage_dict(self, line):
        coverage_dict = {}

        coverage_dict["Filename"] = line[0]
        coverage_dict["Regions"] = line[1]
        coverage_dict["Regions Missed"] = line[2]
        coverage_dict["Regions Coverage"] = line[3]
        coverage_dict["Functions"] = line[4]
        coverage_dict["Functions Missed"] = line[5]
        coverage_dict["Functions Executed"] = line[6]
        coverage_dict["Lines"] = line[7]
        coverage_dict["Lines Missed"] = line[8]
        coverage_dict["Lines Coverage"] = line[9]
        coverage_dict["Branches"] = line[10]
        coverage_dict["Branches Missed"] = line[11]
        coverage_dict["Branches Coverage"] = line[12]

        return coverage_dict

    def merge_profraw_files(self):
        llvm_profdata_merge = [
            "llvm-profdata", "merge", "-sparse", "-o",
            f"{self.output_file.name}"
        ]
        for profraw in self.profraw_files:
            llvm_profdata_merge.append(profraw)

        subprocess.run(llvm_profdata_merge)

    def get_coverage_report(self):
        llvm_cov_report = [
            "llvm-cov", "report", f"{self.rp_binary}",
            f"-instr-profile={self.output_file.name}"
        ]

        results = subprocess.run(llvm_cov_report,
                                 capture_output=True,
                                 encoding="utf-8")
        self.output_file.close()

        results = results.stdout.split("\n")
        report = []

        for line in results:
            if line.startswith(self.filter_target):
                report.append(self.create_coverage_dict(line.split()))

        return report


def main(args):
    rp_binary = os.path.join(args.build_root, "debug/clang/bin/redpanda")
    coverage_agent = CoverageAgent(args.profraw_files, rp_binary, args.target)

    # merge profraw files into
    coverage_agent.merge_profraw_files()

    # get coverage report
    report = coverage_agent.get_coverage_report()

    # print in json format
    print(json.dumps(report, indent=4, sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=
        "Generate a code coverage report given a list of .profraw files")
    parser.add_argument("profraw_files",
                        metavar=".profraw",
                        type=str,
                        nargs="+",
                        help="a list of .profraw files")
    parser.add_argument("--build-root",
                        type=str,
                        required=True,
                        help="the path to redpanda/vbuild")
    parser.add_argument("--target",
                        type=str,
                        default="src/v",
                        help="get coverage for files in this path")
    args = parser.parse_args()

    main(args)
