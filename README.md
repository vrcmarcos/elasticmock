# ElasticMock

Python Elasticsearch Mock for test purposes

[![Build Status](https://travis-ci.org/vrcmarcos/elasticmock.svg?branch=master)](https://travis-ci.org/vrcmarcos/elasticmock) [![Coverage Status](https://coveralls.io/repos/github/vrcmarcos/elasticmock/badge.svg?branch=master)](https://coveralls.io/github/vrcmarcos/elasticmock?branch=master) [![GitHub version](https://badge.fury.io/gh/vrcmarcos%2Felasticmock.svg)](https://badge.fury.io/gh/vrcmarcos%2Felasticmock) [![Code Health](https://landscape.io/github/vrcmarcos/elasticmock/master/landscape.svg?style=flat)](https://landscape.io/github/vrcmarcos/elasticmock/master) [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/vrcmarcos/elasticmock/master/LICENSE)

## Instalation

```shell
pip install ElasticMock
```

## Usage

To use ElasticMock, decorate your test method with **@elasticmock** decorator:

```python
from unittest import TestCase

from elasticmock import elasticmock


class TestClass(TestCase):

    @elasticmock
    def test_should_return_something_from_elasticsearch(self):
        self.assertIsNotNone(some_function_that_uses_elasticsearch())
```

## Notes:

- The mocked **search** method returns **all available documents** indexed on the index with the requested document type.

## Changelog

#### 1.0.0:
- **elasticmock**: Created **@elasticmock** decorator
- **FakeElasticSearch**: Mocked **exists**, **get**, **get_source**, **index**, **info**, **search** and **ping** method