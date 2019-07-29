from setuptools import setup, find_packages
import os
import glob

with open("README.md", "r") as fh:
    long_description = fh.read()
setup(
    name = "pymysql_utils",
    version = "2.1.0",
    packages = find_packages(),

    # Dependencies on other packages:
    # Couldn't get numpy install to work without
    # an out-of-band: sudo apt-get install python-dev
    setup_requires   = [],
    install_requires = [
                        'mysqlclient>=1.3.14',
                        'configparser>=3.3.0',
                        ],
    tests_require    = ['sentinels>=0.0.6',
                        'nose>=1.0',
                        'shutilwhich>=1.1.0',
                        ],

    # Unit tests; they are initiated via 'python setup.py test'
    test_suite       = 'nose.collector', 

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
