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
        'ducktape@git+https://github.com/redpanda-data/ducktape.git@b61a7fe41cb5156a9d9b3fea5ef3df03ef7aa10a',
        'prometheus-client==0.9.0', 'pyyaml==5.3.1', 'kafka-python==2.0.2',
        'crc32c==2.2', 'confluent-kafka==1.7.0', 'zstandard==0.15.2',
        'xxhash==2.0.2', 'protobuf==3.19.3', 'fastavro==1.4.9',
        'psutil==5.9.0', 'numpy==1.22.3', 'pygal==3.0', 'pytest==7.1.2',
        'jump-consistent-hash==3.2.0',
        'kafkatest@git+https://github.com/apache/kafka.git@058589b03db686803b33052d574ce887fb5cfbd1#egg=kafkatest&subdirectory=tests'
    ],
    scripts=[],
)
