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
        'ducktape@git+https://github.com/redpanda-data/ducktape.git@a60d8e93ac5f13554dae036465a28ece6e407df1',
        'prometheus-client==0.9.0', 'pyyaml==5.3.1', 'kafka-python==2.0.2',
        'crc32c==2.2', 'confluent-kafka==1.7.0', 'zstandard==0.15.2',
        'xxhash==2.0.2', 'protobuf==3.19.3', 'fastavro==1.4.9', 'psutil==5.9.0'
    ],
    scripts=[],
)
