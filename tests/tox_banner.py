# -*- coding: utf-8 -*-

from __future__ import print_function

import platform

import elasticsearch

print(
    "{} {}; ElasticSearch {}".format(
        platform.python_implementation(),
        platform.python_version(),
        elasticsearch.VERSION
    )
)
