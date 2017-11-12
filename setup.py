# @copyright (c) 2002-2016 Acronis International GmbH. All rights reserved.
from setuptools import setup, find_packages
import sys
import os

from importlib.machinery import SourceFileLoader


module = SourceFileLoader("version", os.path.join("aio_pika", "version.py")).load_module()

requires = [
    'shortuuid',
    'pika<0.11',
    'yarl',
]


if sys.version_info < (3, 4):
    raise RuntimeError("aio-pika doesn't support Python version prior 3.4")

if sys.version_info < (3, 5):
    requires.append('typing')


setup(
    name='aio-pika',
    version=module.__version__,
    author=module.__author__,
    author_email=module.team_email,
    license=module.package_license,
    description=module.package_info,
    long_description=open("README.rst").read(),
    platforms="all",
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Internet',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: MacOS',
        'Operating System :: POSIX',
        'Operating System :: Microsoft',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    packages=find_packages(exclude=['tests']),
    install_requires=requires,
    extras_require={
        'develop': [
            'asynctest<0.11',
            'coverage!=4.3',
            'coveralls',
            'pylama',
            'pytest',
            'pytest-asyncio<0.6',
            'pytest-cov',
            'sphinx',
            'sphinx-autobuild',
            'timeout-decorator',
            'tox>=2.4',
        ],
    },
)
