#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name='dea_es',
    version='0.1.0.0',
    description='ElasticSearch module for dea lambdas',
    packages=find_packages(),
    install_requires=[
        'elasticsearch',
        'requests_aws4auth',
        'dea_raijin',
    ]
)
