# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
#
# Invenio is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.

"""Invenio module that provides integration with Flask extensions."""

import os
import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand

readme = open('README.rst').read()
history = open('CHANGES.rst').read()

requirements = [
    # Cerberus>=0.7.1 api changes and is not yet supported
    'Cerberus>=0.7,<0.7.1',
    'Flask-Admin>=1.1.0',
    'Flask-Assets>=0.10',
    'Flask-Babel>=0.9',
    'Flask-Breadcrumbs>=0.2',
    'Flask-Cache>=0.12',
    'Flask-Collect>=1.1.1',
    'Flask-Email>=1.4.4',
    'Flask-Gravatar>=0.4.2',
    'Flask-IIIF>=0.2.0',
    'Flask-Login>=0.2.7',
    'Flask-Menu>=0.2',
    'Flask-OAuthlib>=0.6.0,<0.7',  # quick fix for issue #2158
    'Flask-Principal>=0.4',
    'Flask-RESTful>=0.2.12',
    'Flask-Registry>=0.2',
    'Flask-SQLAlchemy>=2.0',
    'Flask-Script>=2.0.5',
    'Flask-WTF>=0.10.2',
    'Flask>=0.10.1',
    'MySQL-python>=1.2.5',
    'backports.lzma>=0.0.3',
    'celery>=3.1.8',
    'elasticsearch>=1.3.0',
    'fixture>=1.5',
    'intbitset>=2.0.0',
    'invenio-base>=0.3.1',
    'invenio-utils>=0.2.0',
    'lxml>=3.3',
    # FIXME new oauthlib release after 0.7.2 has some compatible problems with
    # the used Flask-Oauthlib version.
    'oauthlib==0.7.2',
    'passlib>=1.6.2',
    'python-dateutil>=1.5',
    'raven>=5.0.0',
    'redis>=2.8.0',
    'requests>=2.4.0',
    'six>=1.7.2',
    'sqlalchemy-utils[encrypted]>=0.31.0',
]

test_requirements = [
    'coverage>=4.0.0',
    'Flask-SSO>=0.2',
    'httpretty>=0.8.10',
    'invenio-access>=0.2.0',
    'invenio-accounts>=0.2.0',
    'invenio-collections>=0.2.0',
    'invenio-records>=0.3.4.post1',
    'invenio-oauth2server>=0.2.0',
    'invenio-testing>=0.1.1',
    'mock>=1.0.1',
    'pytest>=2.8.0',
    'pytest-cov>=2.1.0',
    'pytest-isort>=0.1.0',
    'pytest-pep8>=1.0.6',
    'setuptools>=17.1',
]


class PyTest(TestCommand):

    """PyTest Test."""

    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        """Init pytest."""
        TestCommand.initialize_options(self)
        self.pytest_args = []
        try:
            from ConfigParser import ConfigParser
        except ImportError:
            from configparser import ConfigParser
        config = ConfigParser()
        config.read('pytest.ini')
        self.pytest_args = config.get('pytest', 'addopts').split(' ')

    def finalize_options(self):
        """Finalize pytest."""
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        """Run tests."""
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

# Get the version string. Cannot be done with import!
g = {}
with open(os.path.join('invenio_ext', 'version.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['__version__']

setup(
    name='invenio-ext',
    version=version,
    description=__doc__,
    long_description=readme + '\n\n' + history,
    keywords='invenio TODO',
    license='GPLv2',
    author='CERN',
    author_email='info@invenio-software.org',
    url='https://github.com/inveniosoftware/invenio-ext',
    packages=[
        'invenio_ext',
    ],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=requirements,
    extras_require={
        'docs': [
            'Sphinx>=1.3',
            'sphinx_rtd_theme>=0.1.7'
        ],
        'tests': test_requirements
    },
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2',
        # 'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        # 'Programming Language :: Python :: 3',
        # 'Programming Language :: Python :: 3.3',
        # 'Programming Language :: Python :: 3.4',
        'Development Status :: 1 - Planning',
    ],
    tests_require=test_requirements,
    cmdclass={'test': PyTest},
)
