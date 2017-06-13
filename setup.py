# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

__version__ = '1.3.2'

setup(
    name='ElasticMock',
    version=__version__,
    url='https://github.com/vrcmarcos/elasticmock',
    license='MIT',
    author='Marcos Cardoso',
    author_email='vrcmarcos@gmail.com',
    description='Python Elasticsearch Mock for test purposes',
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'elasticsearch<=1.9.0',
        'mock<=1.0.1'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
