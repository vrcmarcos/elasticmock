# ElasticMock

Python Elasticsearch Mock for test purposes

## Usage

To use ElasticMock, you can decorate your TestClass with *patch* decorator:

```python
from unittest import TestCase

from mock import patch
from elasticmock import get_elasticmock


@patch('elasticsearch.Elasticsearch', get_elasticmock)
class TestClass(TestCase):

    def test_should_return_something_from_elasticsearch(self):
        self.assertIsNotNone(some_function_that_uses_elasticsearch())
```

## Changelog

#### 0.0.1:
- **elasticmock**: Mocked **exists**, **get**, **index** and **ping** method