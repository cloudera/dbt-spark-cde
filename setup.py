#!/usr/bin/env python
import os
import sys
import re

from setuptools import find_namespace_packages, setup
# require python 3.7 or newer
if sys.version_info < (3, 7):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.7 or higher.")
    sys.exit(1)


try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)


# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md"), "r", encoding="utf8") as f:
    long_description = f.read()


# get this package's version from dbt/adapters/<name>/__version__.py
def _get_plugin_version_dict():
    _version_path = os.path.join(this_directory, "dbt", "adapters", "spark_cde", "__version__.py")
    _semver = r"""(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"""
    _pre = r"""((?P<prekind>a|b|rc)(?P<pre>\d+))?"""
    _version_pattern = fr"""version\s*=\s*["']{_semver}{_pre}["']"""
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f"invalid version at {_version_path}")
        return match.groupdict()


# require a compatible minor version (~=), prerelease if this is a prerelease
def _get_dbt_core_version():
    parts = _get_plugin_version_dict()
    minor = "{major}.{minor}.0".format(**parts)
    pre = parts["prekind"] + "1" if parts["prekind"] else ""
    return f"{minor}{pre}"


package_name = "dbt-spark-cde"
package_version = "1.3.0"
dbt_core_version = _get_dbt_core_version()
description = """The CDE API based Cloudera Spark adapter plugin for dbt"""

odbc_extras = ["pyodbc>=4.0.30"]
pyhive_extras = [
    "PyHive[hive]>=0.6.0,<0.7.0",
    "thrift>=0.11.0,<0.16.0",
]
session_extras = ["pyspark>=3.0.0,<4.0.0"]
all_extras = odbc_extras + pyhive_extras + session_extras

setup(
    name=package_name,
    version=package_version,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Cloudera",
    author_email="innovation-feedback@cloudera.com",
    url="https://github.com/cloudera/dbt-spark-cde",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    data_files=[('', ['dbt/adapters/spark_cde/.env'])],
    include_package_data=True,
    install_requires=[
        "dbt-core~={}".format(dbt_core_version),
        "sqlparams>=3.0.0",
        "requests>=2.28.1",
        "requests-toolbelt>=0.9.1",
        "python-decouple>=3.6"
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: Apache Software License"
    ],
    zip_safe=False,
    python_requires=">=3.7",
)
