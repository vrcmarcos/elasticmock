# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='ElasticMock',
    version='0.0.1',
    url='https://github.com/elasticmock',
    license='GNU GPLv3',
    author='Marcos Cardoso',
    author_email='vrcmarcos@gmail.com',
    description='Python Elasticsearch Mock for test purposes',
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'elasticsearch<=1.9.0'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
