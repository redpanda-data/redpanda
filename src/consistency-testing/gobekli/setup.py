from setuptools import find_packages
from setuptools import setup

from setuptools import setup, find_packages
setup(
    name="gobekli",
    version="0.1",
    packages=find_packages(),
    setup_requires=['setuptools'],
    scripts=['bin/gobekli-run', 'bin/gobekli-chart'],
    install_requires=['aiohttp', 'requests', 'argparse'],
    test_suite="tests",
)
