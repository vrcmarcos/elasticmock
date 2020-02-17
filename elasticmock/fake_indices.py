# -*- coding: utf-8 -*-


class IndicesClient:
    def __init__(self):
        pass

    @staticmethod
    def create(index: str, ignore: Union[int, List[int]]):
        if index in FakeElasticsearch.documents_dict:
            return
        FakeElasticsearch.documents_dict[index] = list()

    @staticmethod
    def refresh(index: str):
        pass

    @staticmethod
    def delete(index: str, ignore: Union[int, List[int]]):
        if index in FakeElasticsearch.documents_dict:
            del FakeElasticsearch.documents_dict[index]

    @staticmethod
    def exists(index: str):
        return index in FakeElasticsearch.documents_dict