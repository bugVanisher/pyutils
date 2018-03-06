#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
    Created by heyu on 17/3/1
"""

try:
    from setuptools import setup
except:
    from distutils.core import setup

setup(
    name="pyutils",
    version="0.0.1",
    author="heyu",
    author_email="gannicus_yu@163.com",
    description="easy and convenient tools written in Python",
    long_description=__doc__,
    install_requires=["MySQL-python", "docopt"],
    url="https://github.com/gannicus-yu/pyutils",
    packages=["myutils"],
    platforms=['all'],
    # test_suite="tests"
)
