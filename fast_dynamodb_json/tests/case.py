# -*- coding: utf-8 -*-

import typing as T
import enum
import dataclasses
import polars as pl
from dynamodb_json import json_util

from ..schema import (
    Integer,
    Float,
    String,
    Binary,
    Bool,
    Null,
    Set,
    List,
    Struct,
)
from ..seder import deserialize
from ..typehint import (
    T_ITEM,
    T_JSON,
    T_SIMPLE_SCHEMA,
    T_POLARS_SCHEMA,
)


@dataclasses.dataclass
class Case:
    item: T_ITEM = dataclasses.field()
    json: T_JSON = dataclasses.field()
    simple_schema: T_SIMPLE_SCHEMA = dataclasses.field()
    polars_schema: T_POLARS_SCHEMA = dataclasses.field()

    def test_dynamodb_json(self):
        res1 = json_util.dumps(self.item, as_dict=True)
        assert res1 == self.json

        res2 = json_util.loads(res1)
        assert res2 == self.item

    def test_deserialize(self):
        res = deserialize(records=[self.json], simple_schema=self.simple_schema)
        assert res[0] == self.item


class CaseEnum:
    case1 = Case(
        item={"id": 1},
        json={"id": {"N": "1"}},
        simple_schema={"id": Integer()},
        polars_schema={"id": pl.Int64()},
    )

    @classmethod
    def items(cls) -> T.Iterable[T.Tuple[str, Case]]:
        for k, v in cls.__dict__.items():
            if isinstance(v, Case):
                yield k, v
