# -*- coding: utf-8 -*-

import typing as T
import dataclasses


@dataclasses.dataclass
class BaseType:
    pass


DATA_TYPE = T.TypeVar("DATA_TYPE", bound=BaseType)


@dataclasses.dataclass
class Integer(BaseType):
    pass


@dataclasses.dataclass
class Float(BaseType):
    pass


@dataclasses.dataclass
class String(BaseType):
    pass


@dataclasses.dataclass
class Binary(BaseType):
    pass


@dataclasses.dataclass
class Bool(BaseType):
    pass


@dataclasses.dataclass
class Null(BaseType):
    pass


@dataclasses.dataclass
class Set(BaseType):
    itype: BaseType = dataclasses.field()


@dataclasses.dataclass
class List(BaseType):
    itype: BaseType = dataclasses.field()


@dataclasses.dataclass
class Struct(BaseType):
    types: T.Dict[str, BaseType] = dataclasses.field()
