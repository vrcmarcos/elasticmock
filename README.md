# ElasticMock

Python Elasticsearch Mock for test purposes

[![Build Status](https://travis-ci.org/vrcmarcos/elasticmock.svg?branch=master)](https://travis-ci.org/vrcmarcos/elasticmock) [![Coverage Status](https://coveralls.io/repos/github/vrcmarcos/elasticmock/badge.svg?branch=master)](https://coveralls.io/github/vrcmarcos/elasticmock?branch=master) [![PyPI version](https://badge.fury.io/py/ElasticMock.svg)](https://badge.fury.io/py/ElasticMock) [![Code Health](https://landscape.io/github/vrcmarcos/elasticmock/master/landscape.svg?style=flat)](https://landscape.io/github/vrcmarcos/elasticmock/master) [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/vrcmarcos/elasticmock/master/LICENSE)

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
