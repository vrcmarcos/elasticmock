# ElasticMock

Python Elasticsearch Mock for test purposes

[![Build Status](https://travis-ci.org/vrcmarcos/elasticmock.svg?branch=master)](https://travis-ci.org/vrcmarcos/elasticmock) [![Coverage Status](https://coveralls.io/repos/github/vrcmarcos/elasticmock/badge.svg?branch=master)](https://coveralls.io/github/vrcmarcos/elasticmock?branch=master) [![PyPI version](https://badge.fury.io/py/ElasticMock.svg)](https://badge.fury.io/py/ElasticMock) [![GitHub license](https://img.shields.io/github/license/vrcmarcos/elasticmock)](https://github.com/vrcmarcos/elasticmock/blob/master/LICENSE) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ElasticMock) ![ElasticSearch Version](https://img.shields.io/badge/elasticsearch-1%20%7C%202%20%7C%205%20%7C%206%20%7C%207-blue) 

![Libraries.io dependency status for latest release](https://img.shields.io/librariesio/release/pypi/elasticmock) [![Downloads](https://pepy.tech/badge/elasticmock/month)](https://pepy.tech/project/elasticmock/month)

## Installation

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

### Custom Behaviours

You can also force the behaviour of the ElasticSearch instance by importing the `elasticmock.behaviour` module:

```python
from unittest import TestCase

from elasticmock import behaviour


class TestClass(TestCase):

    ...

    def test_should_return_internal_server_error_when_simulate_server_error_is_true(self):
        behaviour.server_failure.enable()
        ...
        behaviour.server_failure.disable()
```

You can also disable all behaviours by calling `behaviour.disable_all()` (Consider put this in your `def tearDown(self)` method)

#### Available Behaviours

* `server_failure`: Will make all calls to ElasticSearch returns the following error message:
    ```python
    {
        'status_code': 500,
        'error': 'Internal Server Error'
    }
    ```

## Code example

Let's say you have a prod code snippet like this one:

```python
import elasticsearch

class FooService:

    def __init__(self):
        self.es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    def create(self, index, body):
        es_object = self.es.index(index, body)
        return es_object.get('_id')

    def read(self, index, id):
        es_object = self.es.get(index, id)
        return es_object.get('_source')

```

Than you should be able to test this class by mocking ElasticSearch using the following test class:

```python
from unittest import TestCase
from elasticmock import elasticmock
from foo.bar import FooService

class FooServiceTest(TestCase):

    @elasticmock
    def should_create_and_read_object(self):
        # Variables used to test
        index = 'test-index'
        expected_document = {
            'foo': 'bar'
        }

        # Instantiate service
        service = FooService()

        # Index document on ElasticSearch
        id = service.create(index, expected_document)
        self.assertIsNotNone(id)

        # Retrive dpcument from ElasticSearch
        document = service.read(index, id)
        self.assertEquals(expected_document, document)

```

## Notes:

- The mocked **search** method returns **all available documents** indexed on the index with the requested document type.
- The mocked **suggest** method returns the exactly suggestions dictionary passed as body serialized in Elasticsearch.suggest response. **Atention:** If the term is an *int*, the suggestion will be ```python term + 1```. If not, the suggestion will be formatted as ```python {0}_suggestion.format(term) ```.
Example:
	- **Suggestion Body**:
	```python
    suggestion_body = {
        'suggestion-string': {
            'text': 'test_text',
            'term': {
                'field': 'string'
            }
        },
        'suggestion-id': {
            'text': 1234567,
            'term': {
                'field': 'id'
            }
        }
    }
    ```
    - **Suggestion Response**:
    ```python
    {
        'suggestion-string': [
            {
                'text': 'test_text',
                'length': 1,
                'options': [
                    {
                        'text': 'test_text_suggestion',
                        'freq': 1,
                        'score': 1.0
                    }
                ],
                'offset': 0
            }
        ],
        'suggestion-id': [
            {
                'text': 1234567,
                'length': 1,
                'options': [
                    {
                        'text': 1234568,
                        'freq': 1,
                        'score': 1.0
                    }
                ],
                'offset': 0
            }
        ],
    }
    ```

## Testing

```bash
python setup.py test
```

## Changelog

#### 1.7.0:
- [Add shards skipped to search and count](https://github.com/vrcmarcos/elasticmock/pull/56) (Thanks [@philtweir](https://github.com/philtweir))
- [Allow 'match_all' queries in FakeSearch](https://github.com/vrcmarcos/elasticmock/pull/54) (Thanks [@jankislinger](https://github.com/jankislinger))
- [Query using nested attributes](https://github.com/vrcmarcos/elasticmock/pull/55) (Thanks [@jankislinger](https://github.com/jankislinger))
- [New features: range, size, aggregations](https://github.com/vrcmarcos/elasticmock/pull/57) (Thanks [@jankislinger](https://github.com/jankislinger))
- [Adding "should" and "minimum_should_match" to QueryType](https://github.com/vrcmarcos/elasticmock/pull/62) (Thanks [@lunarie16](https://github.com/lunarie16))

#### 1.6.2:
- [Add must to query type](https://github.com/vrcmarcos/elasticmock/pull/47) (Thanks [@cuent](https://github.com/cuent))
- [Add match all query type](https://github.com/vrcmarcos/elasticmock/pull/48) (Thanks [@cuent](https://github.com/cuent))

#### 1.6.1:
- Fix Twine README.md

#### 1.6.0:
- [Implements several basic search types](https://github.com/vrcmarcos/elasticmock/pull/42) (Thanks [@KyKoPho](https://github.com/KyKoPho))
- [Allow ignoring of missing documents (404) for get and delete](https://github.com/vrcmarcos/elasticmock/pull/44) (Thanks [@joosterman](https://github.com/joosterman))

#### 1.5.1:
- [Fix tests for es > 7](https://github.com/vrcmarcos/elasticmock/pull/38) (Thanks [@chesstrian](https://github.com/chesstrian))

#### 1.5.0:
- [**FakeElasticSearch**: Mocked **indices** property](https://github.com/vrcmarcos/elasticmock/issues/22)
  - **FakeIndicesClient**: Mocked **create**, **exists**, **refresh** and **delete** methods
- [**FakeElasticSearch**: Mocked **cluster** property](https://github.com/vrcmarcos/elasticmock/issues/8)
  - **FakeClusterClient**: Mocked **health** method

#### 1.4.0

- [Fix es.index regression issue](https://github.com/vrcmarcos/elasticmock/issues/34)
- [Add 'Force Server Failure' feature as requested](https://github.com/vrcmarcos/elasticmock/issues/28)
- Reformat code to be compliant with PEP8
- Add support to Python 3.8

#### 1.3.7

- [Adding fix for updating existing doc using index](https://github.com/vrcmarcos/elasticmock/pull/32) (Thanks [@adityaghosh](https://github.com/adityaghosh))
- [Added bulk method](https://github.com/vrcmarcos/elasticmock/pull/30) (Thanks [@charl-van-niekerk](https://github.com/charl-van-niekerk))
- [Add default value to doc_type in index method as it is by default set to '\_doc'](https://github.com/vrcmarcos/elasticmock/pull/27) (Thanks [@mohantyashish109](https://github.com/mohantyashish109))
- [Add support for Python 3.7](https://github.com/vrcmarcos/elasticmock/pull/25) (Thanks [@asherf](https://github.com/asherf))

#### 1.3.6

- [Fix installation issue](https://github.com/vrcmarcos/elasticmock/pull/20) (Thanks [@tdhopper](https://github.com/tdhopper))

#### 1.3.5

- [Fix 1.3.4 release](https://github.com/vrcmarcos/elasticmock/pull/19) (Thanks [@infinite-Joy](https://github.com/infinite-Joy))

#### 1.3.4

- [Added aggregations to response if requested](https://github.com/vrcmarcos/elasticmock/pull/15) (Thanks [@snakeye](https://github.com/snakeye))
- [Implementing new methods for scrolling](https://github.com/vrcmarcos/elasticmock/pull/17) (Thanks [@tcatrain](https://github.com/tcatrain))

#### 1.3.3

- [Search: doc_type can be a list](https://github.com/vrcmarcos/elasticmock/pull/16) (Thanks [@garncarz](https://github.com/garncarz))
- [Exclude tests package](https://github.com/vrcmarcos/elasticmock/pull/13) (Thanks [@jmlw](https://github.com/jmlw))
- [Make the FakeElasticsearch __init__ signature match the one from Elasticsearch](https://github.com/vrcmarcos/elasticmock/pull/10) (Thanks [@xrmx](https://github.com/xrmx))
- [Improve search and count](https://github.com/vrcmarcos/elasticmock/pull/7) (Thanks [@frivoire](https://github.com/frivoire))

#### 1.3.2

- **elasticmock**: Python 3 fixes (Thanks [@barseghyanartur](https://github.com/barseghyanartur))
- **test**: Add information on testing (Thanks [@barseghyanartur](https://github.com/barseghyanartur))
- **README.md**: Fixed typo (Thanks [@bowlofstew](https://github.com/bowlofstew))

#### 1.3.1

- **elasticmock**: Allow the same arguments to the mock that elasticsearch.Elasticsearch allows (Thanks [@mattbreeden](https://github.com/mattbreeden))

#### 1.3.0:
- **FakeElasticSearch**: Mocked **count** method (Thanks [@TheoResources](https://github.com/TheoResources))

#### 1.2.0:
- **FakeElasticSearch**: Mocked **suggest** method

#### 1.1.1:
- **elasticmock**: Changing the cleanup older FakeElasticSearch's instances order
- **FakeElasticSearch.index**: Changing the method signature to correctly overrides the Elasticsearch.index method

#### 1.1.0:
- **FakeElasticSearch**: Mocked **delete** method

#### 1.0.1:
- **setup.py**: Fixed GitHub link

#### 1.0.0:
- **elasticmock**: Created **@elasticmock** decorator
- **FakeElasticSearch**: Mocked **exists**, **get**, **get_source**, **index**, **info**, **search** and **ping** method
