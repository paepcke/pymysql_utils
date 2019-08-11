from setuptools import setup, find_packages
import os
import glob

# Import the special test runner, which tests
# both mysqlclient and pymysql substrates:

import sys
sys.path.append(os.path.os.path.dirname(__file__))
from pymysql_utils.test_framework import MyTestRunner

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name = "pymysql_utils",
    version = "2.1.3",
    packages = find_packages(),

    # Dependencies on other packages:
    # Couldn't get numpy install to work without
    # an out-of-band: sudo apt-get install python-dev
    setup_requires   = [],
    install_requires = [
                        'mysqlclient>=1.3.14',
                        'PyMySQL>=0.9.3',      # Only needed if needing to run Python-only
                        'configparser>=3.3.0',
                        ],
    tests_require    = ['sentinels>=0.0.6',
                        'shutilwhich>=1.1.0',
                        ],

    # Unit tests; they are initiated via:
    #
    #      'python setup.py test [-v]'
    #
    # If -v is provided, each test case is announced.
    # The test runner is in pymysql_utils.test_framework.py.
    # Must use <package>:<module> notation:

    test_runner      = 'pymysql_utils:test_framework.MyTestRunner',

    # Metadata for upload to PyPI

    author = "Andreas Paepcke",
    author_email = "paepcke@cs.stanford.edu",
    long_description_content_type = "text/markdown",
    description = "Thin wrapper around mysqlclient. Provides Python iterator for queries. Abstracts away cursor.",
    long_description = long_description,
    license = "BSD",
    keywords = "MySQL",
    url = "https://github.com/paepcke/pymysql_utils",   # project home page
    )
