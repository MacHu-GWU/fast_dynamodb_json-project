# -*- coding: utf-8 -*-

import typing as T
import dataclasses

import polars as pl

if T.TYPE_CHECKING:  # pragma: no cover
    from .typehint import T_SIMPLE_SCHEMA, T_POLARS_SCHEMA


@dataclasses.dataclass
class BaseType:

    def to_polars(self) -> T.Union[
        pl.Int64,
        pl.Float64,
        pl.Utf8,
        pl.Binary,
        pl.Boolean,
        pl.Null,
        pl.List,
        pl.Struct,
    ]:
        raise NotImplementedError


DATA_TYPE = T.TypeVar("DATA_TYPE", bound=BaseType)


@dataclasses.dataclass
class Integer(BaseType):
    def to_polars(self) -> pl.Int64:
        return pl.Int64()


@dataclasses.dataclass
class Float(BaseType):
    def to_polars(self) -> pl.Float64:
        return pl.Float64()


@dataclasses.dataclass
class String(BaseType):
    def to_polars(self) -> pl.Utf8:
        return pl.Utf8()


@dataclasses.dataclass
class Binary(BaseType):
    def to_polars(self) -> pl.Binary:
        return pl.Binary()


@dataclasses.dataclass
class Bool(BaseType):
    def to_polars(self) -> pl.Boolean:
        return pl.Boolean()


@dataclasses.dataclass
class Null(BaseType):
    def to_polars(self) -> pl.Null:
        return pl.Null()


@dataclasses.dataclass
class Set(BaseType):
    """
    :param itype: The type of the elements in the set.
    """

    itype: BaseType = dataclasses.field()

    def to_polars(self) -> pl.List:
        return pl.List(self.itype.to_polars())


@dataclasses.dataclass
class List(BaseType):
    """
    :param itype: The type of the elements in the list.
    """

    itype: BaseType = dataclasses.field()

    def to_polars(self) -> pl.List:
        return pl.List(self.itype.to_polars())


@dataclasses.dataclass
class Struct(BaseType):
    """
    :param types: The types of the fields in the struct.
    """

    types: T.Dict[str, BaseType] = dataclasses.field()

    def to_polars(self) -> pl.Struct:
        return pl.Struct({k: v.to_polars() for k, v in self.types.items()})
