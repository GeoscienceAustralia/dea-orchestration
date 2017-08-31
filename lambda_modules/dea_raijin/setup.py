#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name='dea_raijin',
    version='0.1.0.0',
    description='Core lambda processing library',
    packages=find_packages(),
    install_requires=[
        'paramiko>=2.2.1',
        'python-dateutil>=2.6.1',
        'boto3>=1.4.6',
    ]
)
