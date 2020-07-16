#!/usr/bin/env python3

import sys
import os
from setuptools import setup, find_packages

__HERE = os.path.realpath(os.path.dirname(__file__))
os.chdir(__HERE)

with open(os.path.join(__HERE, 'README.md')) as fp:
    long_description = fp.read() + '\n'

with open(os.path.join(__HERE, "requirements.txt")) as fh:
    requires = fh.readlines()

with open(os.path.join(__HERE, "test-requirements.txt")) as fh:
    tests_require = requires + fh.readlines()

setup(
    name='bamboo',
    version="0.98.0",
    description='bamboo - redpanda\'s harness',
    long_description=long_description,
    author='bamboo',
    author_email='hi@vectorizedo.',
    url="vectorized.io/redpanda",
    setup_requires=['setuptools'],
    include_package_data=True,
    install_requires=requires,
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    },
    scripts=[],
    entry_points={'console_scripts': ['bamboo=src.main:main']},
)
