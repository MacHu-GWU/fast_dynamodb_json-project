# -*- coding: utf-8 -*-

import typing as T
import dataclasses

import polars as pl

from .sentinel import NOTHING

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

    def to_dynamodb_json_polars(self) -> pl.Struct:
        raise NotImplementedError


DATA_TYPE = T.TypeVar("DATA_TYPE", bound=BaseType)


@dataclasses.dataclass
class Integer(BaseType):
    default_for_null: T.Any = dataclasses.field(default=NOTHING)

    def to_polars(self) -> pl.Int64:
        return pl.Int64()

    def to_dynamodb_json_polars(self) -> pl.Struct:
        return pl.Struct({"N": pl.Utf8()})


@dataclasses.dataclass
class Float(BaseType):
    default_for_null: T.Any = dataclasses.field(default=NOTHING)

    def to_polars(self) -> pl.Float64:
        return pl.Float64()

    def to_dynamodb_json_polars(self) -> pl.Struct:
        return pl.Struct({"N": pl.Utf8()})


DEFAULT_NULL_STRING = ""
DEFAULT_NULL_BINARY = b""


@dataclasses.dataclass
class String(BaseType):
    default_for_null: T.Any = dataclasses.field(default=DEFAULT_NULL_STRING)

    def to_polars(self) -> pl.Utf8:
        return pl.Utf8()

    def to_dynamodb_json_polars(self) -> pl.Struct:
        return pl.Struct({"S": pl.Utf8()})


@dataclasses.dataclass
class Binary(BaseType):
    default_for_null: T.Any = dataclasses.field(default=DEFAULT_NULL_BINARY)

    def to_polars(self) -> pl.Binary:
        return pl.Binary()

    def to_dynamodb_json_polars(self) -> pl.Struct:
        return pl.Struct({"B": pl.Utf8()})


@dataclasses.dataclass
class Bool(BaseType):
    default_for_null: T.Any = dataclasses.field(default=NOTHING)

    def to_polars(self) -> pl.Boolean:
        return pl.Boolean()

    def to_dynamodb_json_polars(self) -> pl.Struct:
        return pl.Struct({"BOOL": pl.Boolean()})


@dataclasses.dataclass
class Null(BaseType):
    default_for_null: T.Any = dataclasses.field(default=None)

    def to_polars(self) -> pl.Null:
        return pl.Null()

    def to_dynamodb_json_polars(self) -> pl.Struct:
        return pl.Struct({"NULL": pl.Boolean()})


@dataclasses.dataclass
class Set(BaseType):
    """
    :param itype: The type of the elements in the set.
    """

    itype: BaseType = dataclasses.field(default=NOTHING)
    default_for_null: T.Any = dataclasses.field(default_factory=list)

    def __post_init__(self):
        if self.itype is NOTHING:
            raise ValueError("itype is required for Set")

    def to_polars(self) -> pl.List:
        return pl.List(self.itype.to_polars())

    def to_dynamodb_json_polars(self) -> pl.Struct:
        if isinstance(self.itype, String):
            field = "SS"
        elif isinstance(self.itype, Integer):
            field = "NS"
        elif isinstance(self.itype, Float):
            field = "NS"
        elif isinstance(self.itype, Binary):
            field = "BS"
        else:
            raise NotImplementedError
        return pl.Struct({field: pl.List(pl.Utf8())})


@dataclasses.dataclass
class List(BaseType):
    """
    :param itype: The type of the elements in the list.
    """

    itype: BaseType = dataclasses.field(default=NOTHING)
    default_for_null: T.Any = dataclasses.field(default_factory=list)

    def __post_init__(self):
        if self.itype is NOTHING:
            raise ValueError("itype is required for List")

    def to_polars(self) -> pl.List:
        return pl.List(self.itype.to_polars())

    def to_dynamodb_json_polars(self) -> pl.Struct:
        return pl.Struct({"L": pl.List(self.itype.to_dynamodb_json_polars())})


@dataclasses.dataclass
class Struct(BaseType):
    """
    :param types: The types of the fields in the struct.
    """

    types: T.Dict[str, BaseType] = dataclasses.field(default=NOTHING)

    def __post_init__(self):
        if self.types is NOTHING:
            raise ValueError("types is required for Struct")

    def to_polars(self) -> pl.Struct:
        return pl.Struct({k: v.to_polars() for k, v in self.types.items()})

    def to_dynamodb_json_polars(self) -> pl.Struct:
        return pl.Struct(
            {
                "M": pl.Struct(
                    {k: v.to_dynamodb_json_polars() for k, v in self.types.items()}
                )
            }
        )
