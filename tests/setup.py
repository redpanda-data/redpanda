from setuptools import find_packages
from setuptools import setup

setup(
    name='rptest',
    version='0',
    provides=['rptest'],
    author='vectorized',
    author_email='hi@vectorized.io',
    description='vectorized internal testing',
    packages=find_packages(),
    setup_requires=['setuptools'],
    package_data={'': ['*.md']},
    include_package_data=True,
    install_requires=[
        'ducktape@git+https://github.com/vectorizedio/ducktape.git@master',
        'prometheus-client==0.9.0',
        'pyyaml==5.3.1',
        'kafka-python==2.0.2',
        'crc32c==2.2',
        'confluent-kafka==1.6.1',
    ],
    scripts=[],
)
