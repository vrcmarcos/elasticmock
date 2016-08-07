# -*- coding: utf-8 -*-

import random
import string

DEFAULT_ELASTICSEARCH_ID_SIZE = 20
CHARSET_FOR_ELASTICSEARCH_ID = string.ascii_letters + string.digits


def get_random_id(size=DEFAULT_ELASTICSEARCH_ID_SIZE):
    return ''.join(random.choice(CHARSET_FOR_ELASTICSEARCH_ID) for _ in range(size))
