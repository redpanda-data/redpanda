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
    setup_requires=['setuptools', 'grpcio-tools==1.53'],
    package_data={'': ['*.md']},
    include_package_data=True,
    install_requires=[
        'ducktape@git+https://github.com/redpanda-data/ducktape.git@979fe00585e616ce23b1f0afadaeb0268ca327a8',
        'prometheus-client==0.9.0', 'pyyaml==6.0.1', 'kafka-python==2.0.2',
        'crc32c==2.2', 'confluent-kafka==2.0.2', 'zstandard==0.15.2',
        'xxhash==2.0.2', 'protobuf==4.21.8', 'fastavro==1.4.9',
        'psutil==5.9.0', 'numpy==1.22.3', 'pygal==3.0', 'pytest==7.1.2',
        'jump-consistent-hash==3.2.0', 'azure-storage-blob==12.14.1',
        'kafkatest@git+https://github.com/apache/kafka.git@3.2.0#egg=kafkatest&subdirectory=tests',
        'grpcio==1.57.0', 'grpcio-tools==1.57', 'grpcio-status==1.57.0',
        'cachetools==5.3.1', 'google-api-core==2.11.1', 'google-auth==2.22.0',
        'googleapis-common-protos==1.60.0', 'google.cloud.compute==1.14.0',
        'proto-plus==1.22.3', 'rsa==4.9'
    ],
    scripts=[],
)
