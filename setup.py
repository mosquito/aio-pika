# @copyright (c) 2002-2016 Acronis International GmbH. All rights reserved.
from setuptools import setup, find_packages


setup(
    name='aio-pika',
    version='0.5.3',
    author="Dmitry Orlov <me@mosquito.su>",
    author_email="me@mosquito.su",
    license="Apache Software License",
    description="Wrapper for the PIKA for asyncio and humans.",
    long_description=open("README.rst").read(),
    platforms="all",
    classifiers=(
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
        'Programming Language :: Python :: Implementation :: CPython',
    ),
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'asyncio',
        'shortuuid',
        'pika<0.11',
        'slimurl>=0.7.6',
    ],
)
