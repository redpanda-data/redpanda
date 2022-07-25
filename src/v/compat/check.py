#!/usr/bin/python3.8

# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import os


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


print("Reading version compatibility table")
f = open('persistence_compat.json')
compat = json.load(f)

versions = compat['all_versions']

print("Downloading versions")
for v in versions:
    print("downloading version ", v, "...")
    print("downloading corpus for version", v, "...")

print("Building compat table")
compat_table = [[True for x in range(len(versions))]
                for y in range(len(versions))]
for version, not_compat_list in compat['not_compatbile'].items():
    version_compat_row = compat_table[versions.index(version)]
    for not_compat in not_compat_list:
        if not_compat[0] == '>':
            until = versions.index(not_compat[1:].strip()) + 1
            for i in range(until, len(versions)):
                version_compat_row[i] = False
        elif not_compat[0] == '<':
            starting = versions.index(not_compat[1:].strip())
            for i in range(0, starting):
                version_compat_row[i] = False
        elif not_compat[0] == '+':
            add = versions.index(not_compat[1:].strip())
            version_compat_row[add] = True
        else:
            print("unkown indicator ", not_compat)

for i, compat in enumerate(compat_table):
    for j, is_compatible in enumerate(compat):
        is_upgrade = j < i
        is_downgrade = i < j
        if is_compatible:
            if is_upgrade:
                print(
                    f"UPGRADE     compat between {versions[i]} and {versions[j]}: ./bin/compat-{versions[i]} -r corpus/{versions[j]}"
                )
            elif is_downgrade:
                print(
                    f"DOWNGRADE   compat between {versions[i]} and {versions[j]}: ./bin/compat-{versions[i]} -r corpus/{versions[j]}"
                )
            else:
                print(
                    f"SELFCHECK   {versions[i]}: ./compat-{versions[i]} -r corpus/{versions[j]}"
                )
            exit_code = os.system(
                f"./bin/compat-{versions[i]} -r ./corpus/{versions[j]}")
            if exit_code == 0:
                print(
                    f"{bcolors.OKGREEN}CHECK OK: ./bin/compat-{versions[i]} -r ./corpus/{versions[j]}{bcolors.ENDC}"
                )
            else:
                print(
                    f"{bcolors.FAIL}CHECK ERROR: ./bin/compat-{versions[i]} -r ./corpus/{versions[j]}{bcolors.ENDC}"
                )
        else:
            pass  # what to do if incompatible? is compatibility an error? what error to expect?
    print('\n')
