# -*- coding: utf-8 -*-

import setuptools

__version__ = '1.3.3'

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='ElasticMock',
    version=__version__,
    author='Marcos Cardoso',
    author_email='vrcmarcos@gmail.com',
    description='Python Elasticsearch Mock for test purposes',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/vrcmarcos/elasticmock',
    packages=setuptools.find_packages(exclude=('tests')),
    install_requires=[
        'elasticsearch',
        'mock'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        "License :: OSI Approved :: MIT License",
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
