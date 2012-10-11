#!/usr/bin/env python
# Copyright (c) 2012 Yummy Bian <yummy.bian#gmail.com>. 
#
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.

import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

def list_files(path):
    for fn in os.listdir(path):
        if fn.startswith('.'):
            continue
        fn = os.path.join(path, fn)
        if os.path.isfile(fn):
            yield fn

setup(
    name = 'consistent_hash',
    version='0.1',
    author="Yummy Bian",
    author_email="yummy.bian@gmail.com",
    url="https://github.com/yummybian",
    packages=['consistent_hash'],
    platforms=["Any"],
    license="BSD",
    keywords='consistent hash hashing',
    description="Implements consistent hashing with Python (using md5 as hashing function).",
)




