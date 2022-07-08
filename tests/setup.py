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
        'ducktape@git+https://github.com/redpanda-data/ducktape.git@59aa1a41bea42a7f2bc8eaf3bc2a9437dd367fe7',
        'prometheus-client==0.9.0', 'pyyaml==5.3.1', 'kafka-python==2.0.2',
        'crc32c==2.2', 'confluent-kafka==1.7.0', 'zstandard==0.15.2',
        'xxhash==2.0.2', 'protobuf==3.19.3', 'fastavro==1.4.9',
        'psutil==5.9.0', 'numpy==1.22.3',
        'kafkatest@git+https://github.com/apache/kafka.git@97671528ba54a138e16f41cdc0ed9c77a83ffccf#egg=kafkatest&subdirectory=tests'
    ],
    scripts=[],
)
