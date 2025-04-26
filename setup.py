#!/usr/bin/env python

import sys
import ast
import os
import re

from setuptools import setup

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('ibapi_wrapper/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

if sys.version_info < (3,1):
    sys.exit("Only Python 3.1 and greater is supported")

setup(
    name='ibapi_wrapper',
    version=version,
    packages=['ibapi_wrapper'],
    url='https://github.com/atreyuxtrading/atreyu-backtrader-api',
    license='Simplified BSD license',
    author='Atreyu Trading',
    author_email='info@atreyugroup.com',
    description='Python IB API for backtrader'
)
