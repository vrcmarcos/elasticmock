# -*- coding: utf-8 -*-

from elasticsearch.transport import Transport


class FakeTransport(Transport):

    def __init__(self, fake_elastic_instance, hosts):
        super().__init__(hosts)
        self.fake_elastic_instance = fake_elastic_instance

    def perform_request(self, method, url, params=None, body=None):
        import ipdb; ipdb.set_trace()
        method = url.replace("/_", "")
        if method == "bulk":
            data = self.fake_elastic_instance.bulk(body=body)
        else:
            raise NotImplementedError("You MUST implement method [{0}] on FakeTransport class!".format(method))
        return True, data

