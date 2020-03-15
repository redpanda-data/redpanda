from setuptools import find_packages
from setuptools import setup

setup(
    name='rp',
    version='0',
    provides=['rp'],
    author='vectorized',
    author_email='hi@vectorized.io',
    description='vectorized internal testing',
    packages=find_packages(),
    setup_requires=['setuptools'],
    package_data={'': ['*.md']},
    include_package_data=True,
    install_requires=['kafkatest', 'pyyaml'],
    scripts=[],
)
