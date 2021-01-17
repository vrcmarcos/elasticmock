# -*- coding: utf-8 -*-

import setuptools

__version__ = '1.7.0'

# read the contents of your readme file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name='ElasticMock',
    version=__version__,
    author='Marcos Cardoso',
    author_email='vrcmarcos@gmail.com',
    description='Python Elasticsearch Mock for test purposes',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/vrcmarcos/elasticmock',
    packages=setuptools.find_packages(exclude=('tests')),
    install_requires=[
        'elasticsearch',
        'mock',
        'python-dateutil',
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        "License :: OSI Approved :: MIT License",
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
