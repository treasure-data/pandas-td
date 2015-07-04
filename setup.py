#!/usr/bin/env python

import re
from setuptools import setup, find_packages

def read_version():
    with open("pandas_td/version.py") as f:
        m = re.match(r'__version__ = "([^\"]*)"', f.read())
        return m.group(1)

setup(
    name="pandas-td",
    version=read_version(),
    description="Pandas extension for Treasure Data",
    author="Treasure Data, Inc.",
    author_email="support@treasure-data.com",
    url="https://github.com/treasure-data/pandas-td",
    install_requires=open("requirements.txt").read().splitlines(),
    packages=find_packages(),
    license="Apache License 2.0",
    platforms="Posix; MacOS X; Windows",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Framework :: IPython",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development",
    ],
)
