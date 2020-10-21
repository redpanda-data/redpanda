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
        'absl-py==0.10.0',
        'ansible==2.10.1',
        'click==7.1.2',
        'crc32c==2.1',
        'docker==4.3.1',
        'docker-compose==1.27.4',
        'gitpython==3.1.9',
        'google-cloud-build==1.1.0',
        'Jinja2==2.11.2',
        'jmespath==0.10.0',
        'paramiko==2.7.2',
        'peewee==3.13.3',
        'psutil==5.7.2',
        'pyjks==20.0.0',
        'pylddwrap==1.0.2',
        'pyyaml==5.3.1',
        'requests==2.24.0',
        'selinux==0.2.1',
        'yapf==0.30.0',
    ],
    scripts=[],
    entry_points={
        'console_scripts': [
            'vtools=vtools.main:main',
        ],
    },
)
