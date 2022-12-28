import os
from setuptools import setup, find_packages
from importlib.machinery import SourceFileLoader


module = SourceFileLoader(
    "version", os.path.join("aio_pika", "version.py")
).load_module()


setup(
    name="aio-pika",
    version=module.__version__,
    author=module.__author__,
    author_email=module.team_email,
    license=module.package_license,
    description=module.package_info,
    long_description=open("README.rst").read(),
    platforms="all",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Internet",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: Microsoft",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    packages=find_packages(exclude=["tests"]),
    package_data={"aio_pika": ["py.typed"]},
    install_requires=[
        "aiormq~=6.6.3",
        "yarl",
        'typing_extensions; python_version < "3.8.0"'
    ],
    python_requires=">3.6, <4",
    extras_require={
        "develop": [
            "aiomisc~=16.0",
            "coverage!=4.3",
            "coveralls",
            "pylava",
            "pytest",
            "pytest-cov",
            "shortuuid",
            "nox",
            "sphinx",
            "sphinx-autobuild",
            "timeout-decorator",
            "tox>=2.4",
        ],
    },
    project_urls={
        "Documentation": "https://aio-pika.readthedocs.org/",
        "Source": "https://github.com/mosquito/aio-pika",
    },
)
