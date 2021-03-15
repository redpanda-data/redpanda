# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from setuptools import find_packages
from setuptools import setup

from setuptools import setup, find_packages

setup(
    name="gobekli",
    version="0.1",
    packages=find_packages(),
    setup_requires=['setuptools'],
    scripts=['bin/gobekli-run', 'bin/gobekli-report'],
    install_requires=['aiohttp', 'requests', 'argparse'],
    test_suite="tests",
)
