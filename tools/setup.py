import os

__HERE = os.path.realpath(os.path.dirname(__file__))
os.chdir(__HERE)

with open(os.path.join(__HERE, 'README.md')) as fp:
    LONG_DESCRIPTION = fp.read() + '\n'

from setuptools import find_packages
from setuptools import setup

from vtools import __version__

setup(
    name='vtools',
    version=__version__,
    provides=['vtools'],
    author='vectorized',
    author_email='hi@vectorized.io',
    description='vectorized internal tooling',
    packages=find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    package_data={'': ['*.md']},
    include_package_data=True,
    install_requires=[
        'absl-py',
        'ansible',
        'click',
        'crc32c',
        'docker',
        'docker-compose',
        'gitpython',
        'google-cloud-build',
        'Jinja2',
        'jmespath',
        'paramiko',
        'peewee',
        'pylddwrap',
        'pyyaml',
        'requests',
        'selinux',
        'yapf',
    ],
    scripts=[],
    entry_points={
        'console_scripts': [
            'vtools=vtools.main:main',
        ],
    },
)
