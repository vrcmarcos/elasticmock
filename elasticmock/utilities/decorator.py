# -*- coding: utf-8 -*-


def for_all_methods(decorators, apply_on_public_only=True):
    def decorate(cls):
        for attr in cls.__dict__:

            if apply_on_public_only:
                if attr.startswith('_'):
                    continue

            if callable(getattr(cls, attr)):
                for decorator in decorators:
                    setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate
