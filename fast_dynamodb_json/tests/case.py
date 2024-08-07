# -*- coding: utf-8 -*-

import typing as T
import json
import jsonpickle
import dataclasses

import polars as pl
from dynamodb_json import json_util
from rich import print as rprint

from ..typehint import (
    T_ITEM,
    T_JSON,
    T_SIMPLE_SCHEMA,
    T_POLARS_SCHEMA,
)
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
from ..seder import serialize
from ..deserialize import deserialize


from ..paths import path_expected_json, path_result_json


@dataclasses.dataclass
class Case:
    item: T_ITEM = dataclasses.field()
    json: T_JSON = dataclasses.field()
    simple_schema: T_SIMPLE_SCHEMA = dataclasses.field(default_factory=dict)
    polars_schema: T_POLARS_SCHEMA = dataclasses.field(default_factory=dict)

    def compare(self, expected, result, write_to_file=True):
        if write_to_file:
            try:
                path_expected_json.write_text(
                    json.dumps(expected, indent=4, sort_keys=True)
                )
                path_result_json.write_text(
                    json.dumps(result, indent=4, sort_keys=True)
                )
            except TypeError:
                path_expected_json.write_text(jsonpickle.dumps(expected, indent=4))
                path_result_json.write_text(jsonpickle.dumps(result, indent=4))

        print("---------- Expected ----------")
        rprint(expected)
        print("---------- Result ----------")
        rprint(result)
        if write_to_file:
            print(f"expected: file://{path_expected_json}")
            print(f"result: file://{path_result_json}")
            print(f"diff {path_expected_json} {path_result_json}")

    def test_dynamodb_json(self):
        res1 = json_util.dumps(self.item, as_dict=True)
        self.compare(expected=self.json, result=res1)
        assert res1 == self.json

        res2 = json_util.loads(res1)
        self.compare(expected=self.item, result=res2)
        assert res2 == self.item

    def test_deserialize(self):
        res = deserialize(records=[self.json], simple_schema=self.simple_schema)
        self.compare(expected=self.item, result=res[0])
        assert res[0] == self.item

    def test_serialize(self):
        res = serialize(records=[self.item], simple_schema=self.simple_schema)
        self.compare(expected=self.json, result=res[0])
        assert res[0] == self.json


class CaseEnum:
    case1 = Case(
        item={
            "a_int": 1,
            "a_float": 3.14,
            "a_str": "Alice",
            "a_bin": b"hello",
        },
        json={
            "a_int": {"N": "1"},
            "a_float": {"N": "3.14"},
            "a_str": {"S": "Alice"},
            "a_bin": {"B": "aGVsbG8="},
        },
        simple_schema={
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
            "a_bin": Binary(),
        },
    )

    case2 = Case(
        item={
            "a_int": None,
            "a_float": None,
            "a_str": None,
            "a_bin": None,
        },
        json={
            "a_int": {"NULL": True},
            "a_float": {"NULL": True},
            "a_str": {"NULL": True},
            "a_bin": {"NULL": True},
        },
        simple_schema={
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
            "a_bin": Binary(),
        },
    )

    case3 = Case(
        item={
            "a_int": None,
            "a_float": None,
            "a_str": None,
            "a_bin": None,
        },
        json={
            "a_int": None,
            "a_float": None,
            "a_str": None,
            "a_bin": None,
        },
        simple_schema={
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
            "a_bin": Binary(),
        },
    )

    case4 = Case(
        item={
            "a_int": None,
            "a_float": None,
            "a_str": None,
            "a_bin": None,
        },
        json={
            # should have "a_int", but it is empty
            # should have "a_float", but it is empty
            # should have "a_str", but it is empty
            # should have "a_bin", but it is empty
        },
        simple_schema={
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
            "a_bin": Binary(),
        },
    )

    @classmethod
    def items(cls) -> T.Iterable[T.Tuple[str, Case]]:
        for k, v in cls.__dict__.items():
            if isinstance(v, Case):
                yield k, v
