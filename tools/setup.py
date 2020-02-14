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
        'click',
        'gitpython',
        'pylddwrap',
        'requests',
        'Jinja2',
        'absl-py',
        'paramiko',
        'pyyaml',
        'yapf',
        'crc32c',
        'peewee',
    ],
    scripts=[],
    entry_points={
        'console_scripts': [
            'vtools=vtools.main:main',
        ],
    },
)
