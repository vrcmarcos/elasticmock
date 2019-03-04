# -*- coding: utf-8 -*-

import random
import string
import base64

DEFAULT_ELASTICSEARCH_ID_SIZE = 20
CHARSET_FOR_ELASTICSEARCH_ID = string.ascii_letters + string.digits

DEFAULT_ELASTICSEARCH_SEARCHRESULTPHASE_COUNT = 6

def get_random_id(size=DEFAULT_ELASTICSEARCH_ID_SIZE):
    return ''.join(random.choice(CHARSET_FOR_ELASTICSEARCH_ID) for _ in range(size))

def get_random_scroll_id(size=DEFAULT_ELASTICSEARCH_SEARCHRESULTPHASE_COUNT):
    return base64.b64encode(''.join(get_random_id() for _ in range(size)).encode())
