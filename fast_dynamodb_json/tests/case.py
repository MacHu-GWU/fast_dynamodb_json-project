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
from ..serialize import serialize
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

    # ------------------------------------------------------------------------------
    # Deserialize, some of them also work for serialize
    # ------------------------------------------------------------------------------
    case1 = Case(
        item={
            "a_int": 1,
            "a_float": 3.14,
            "a_str": "Alice",
            "a_bin": b"hello",
            "a_bool": False,
            "a_null": None,
        },
        json={
            "a_int": {"N": "1"},
            "a_float": {"N": "3.14"},
            "a_str": {"S": "Alice"},
            "a_bin": {"B": "aGVsbG8="},
            "a_bool": {"BOOL": False},
            "a_null": {"NULL": True},
        },
        simple_schema={
            "a_int": Integer(default_for_null=-999),
            "a_float": Float(default_for_null=-999.999),
            "a_str": String(default_for_null="NA"),
            "a_bin": Binary(default_for_null=b"NA"),
            "a_bool": Bool(default_for_null=False),
            "a_null": Null(),
        },
    )

    case2 = Case(
        item={
            "a_int": None,
            "a_float": None,
            "a_str": None,
            "a_bin": None,
            "a_bool": None,
            "a_null": None,
        },
        json={
            "a_int": {"NULL": True},
            "a_float": {"NULL": True},
            "a_str": {"NULL": True},
            "a_bin": {"NULL": True},
            "a_bool": {"NULL": True},
            "a_null": {"NULL": True},
        },
        simple_schema={
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
            "a_bin": Binary(),
            "a_bool": Bool(),
            "a_null": Null(),
        },
    )

    case3 = Case(
        item={
            "a_int": None,
            "a_float": None,
            "a_str": None,
            "a_bin": None,
            "a_bool": None,
            "a_null": None,
        },
        json={
            "a_int": None,
            "a_float": None,
            "a_str": None,
            "a_bin": None,
            "a_bool": None,
            "a_null": None,
        },
        simple_schema={
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
            "a_bin": Binary(),
            "a_bool": Bool(),
            "a_null": Null(),
        },
    )

    case4 = Case(
        item={
            "a_int": None,
            "a_float": None,
            "a_str": None,
            "a_bin": None,
            "a_bool": None,
            "a_null": None,
        },
        json={
            # should have "a_int", but it is empty
            # should have "a_float", but it is empty
            # should have "a_str", but it is empty
            # should have "a_bin", but it is empty
            # should have "a_bool", but it is empty
            # should have "a_null", but it is empty
        },
        simple_schema={
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
            "a_bin": Binary(),
            "a_bool": Bool(),
            "a_null": Null(),
        },
    )

    case5 = Case(
        item={
            "a_int_set": [1, 2, 3],
            "a_float_set": [1.1, 2.2, 3.3],
            "a_str_set": ["a", "b", "c"],
            "a_binary_set": [b"hello", b"world"],
            "a_int_list": [1, 2, 3],
            "a_float_list": [1.1, 2.2, 3.3],
            "a_str_list": ["a", "b", "c"],
            "a_binary_list": [b"hello", b"world"],
        },
        json={
            "a_int_set": {"NS": ["1", "2", "3"]},
            "a_float_set": {"NS": ["1.1", "2.2", "3.3"]},
            "a_str_set": {"SS": ["a", "b", "c"]},
            "a_binary_set": {"BS": ["aGVsbG8=", "d29ybGQ="]},
            "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
            "a_float_list": {"L": [{"N": "1.1"}, {"N": "2.2"}, {"N": "3.3"}]},
            "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
            "a_binary_list": {"L": [{"B": "aGVsbG8="}, {"B": "d29ybGQ="}]},
        },
        simple_schema={
            "a_int_set": Set(Integer()),
            "a_float_set": Set(Float()),
            "a_str_set": Set(String()),
            "a_binary_set": Set(Binary()),
            "a_int_list": List(Integer()),
            "a_float_list": List(Float()),
            "a_str_list": List(String()),
            "a_binary_list": List(Binary()),
        },
    )

    case6 = Case(
        item={
            "a_int_set": None,
            "a_float_set": None,
            "a_str_set": None,
            "a_binary_set": None,
            "a_int_list": None,
            "a_float_list": None,
            "a_str_list": None,
            "a_binary_list": None,
        },
        json={
            "a_int_set": {"NULL": True},
            "a_float_set": {"NULL": True},
            "a_str_set": {"NULL": True},
            "a_binary_set": {"NULL": True},
            "a_int_list": {"NULL": True},
            "a_float_list": {"NULL": True},
            "a_str_list": {"NULL": True},
            "a_binary_list": {"NULL": True},
        },
        simple_schema={
            "a_int_set": Set(Integer()),
            "a_float_set": Set(Float()),
            "a_str_set": Set(String()),
            "a_binary_set": Set(Binary()),
            "a_int_list": List(Integer()),
            "a_float_list": List(Float()),
            "a_str_list": List(String()),
            "a_binary_list": List(Binary()),
        },
    )

    case7 = Case(
        item={
            "a_int_set": None,
            "a_float_set": None,
            "a_str_set": None,
            "a_binary_set": None,
            "a_int_list": None,
            "a_float_list": None,
            "a_str_list": None,
            "a_binary_list": None,
        },
        json={
            "a_int_set": None,
            "a_float_set": None,
            "a_str_set": None,
            "a_binary_set": None,
            "a_int_list": None,
            "a_float_list": None,
            "a_str_list": None,
            "a_binary_list": None,
        },
        simple_schema={
            "a_int_set": Set(Integer()),
            "a_float_set": Set(Float()),
            "a_str_set": Set(String()),
            "a_binary_set": Set(Binary()),
            "a_int_list": List(Integer()),
            "a_float_list": List(Float()),
            "a_str_list": List(String()),
            "a_binary_list": List(Binary()),
        },
    )

    case8 = Case(
        item={
            "a_int_set": None,
            "a_float_set": None,
            "a_str_set": None,
            "a_binary_set": None,
            "a_int_list": None,
            "a_float_list": None,
            "a_str_list": None,
            "a_binary_list": None,
        },
        json={
            # should have "a_int_set", but it is empty
            # should have "a_float_set", but it is empty
            # should have "a_str_set", but it is empty
            # should have "a_binary_set", but it is empty
            # should have "a_int_list", but it is empty
            # should have "a_float_list", but it is empty
            # should have "a_str_list", but it is empty
            # should have "a_binary_list", but it is empty
        },
        simple_schema={
            "a_int_set": Set(Integer()),
            "a_float_set": Set(Float()),
            "a_str_set": Set(String()),
            "a_binary_set": Set(Binary()),
            "a_int_list": List(Integer()),
            "a_float_list": List(Float()),
            "a_str_list": List(String()),
            "a_binary_list": List(Binary()),
        },
    )

    # empty set / list
    case9 = Case(
        item={
            "a_int_set": [],
            "a_float_set": [],
            "a_str_set": [],
            "a_binary_set": [],
            "a_int_list": [],
            "a_float_list": [],
            "a_str_list": [],
            "a_binary_list": [],
        },
        json={
            "a_int_set": {"NS": []},
            "a_float_set": {"NS": []},
            "a_str_set": {"SS": []},
            "a_binary_set": {"BS": []},
            "a_int_list": {"L": []},
            "a_float_list": {"L": []},
            "a_str_list": {"L": []},
            "a_binary_list": {"L": []},
        },
        simple_schema={
            "a_int_set": Set(Integer()),
            "a_float_set": Set(Float()),
            "a_str_set": Set(String()),
            "a_binary_set": Set(Binary()),
            "a_int_list": List(Integer()),
            "a_float_list": List(Float()),
            "a_str_list": List(String()),
            "a_binary_list": List(Binary()),
        },
    )

    case10 = Case(
        item={
            "a_struct": {
                "a_int": 123,
                "a_float": 3.14,
                "a_str": "alice",
                "a_binary": b"hello",
                "a_bool": False,
                "a_null": None,
            },
        },
        json={
            "a_struct": {
                "M": {
                    "a_int": {"N": "123"},
                    "a_float": {"N": "3.14"},
                    "a_str": {"S": "alice"},
                    "a_binary": {"B": "aGVsbG8="},
                    "a_bool": {"BOOL": False},
                    "a_null": {"NULL": True},
                }
            },
        },
        simple_schema={
            "a_struct": Struct(
                {
                    "a_int": Integer(),
                    "a_float": Float(),
                    "a_str": String(),
                    "a_binary": Binary(),
                    "a_bool": Bool(),
                    "a_null": Null(),
                }
            ),
        },
    )

    case11 = Case(
        item={
            "a_list_of_struct": [
                {"a_int": 123, "a_str": "alice"},
                {"a_int": 456, "a_str": "bob"},
            ],
            "a_list_of_list_of_struct": [
                [
                    {"a_int": 123, "a_str": "alice"},
                    {"a_int": 456, "a_str": "bob"},
                ],
                [
                    {"a_int": 789, "a_str": "cathy"},
                    {"a_int": 101112, "a_str": "david"},
                ],
            ],
            "a_struct_of_list": {
                "a_int_list": [1, 2, 3],
                "a_str_list": ["a", "b", "c"],
            },
            "a_struct_of_struct_of_list": {
                "struct_1": {
                    "a_int_list1": [1, 2, 3],
                    "a_str_list1": ["a", "b", "c"],
                },
                "struct_2": {
                    "a_int_list2": [4, 5, 6],
                    "a_str_list2": ["d", "e", "f"],
                },
            },
        },
        json={
            "a_list_of_struct": {
                "L": [
                    {"M": {"a_int": {"N": "123"}, "a_str": {"S": "alice"}}},
                    {"M": {"a_int": {"N": "456"}, "a_str": {"S": "bob"}}},
                ]
            },
            "a_list_of_list_of_struct": {
                "L": [
                    {
                        "L": [
                            {"M": {"a_int": {"N": "123"}, "a_str": {"S": "alice"}}},
                            {"M": {"a_int": {"N": "456"}, "a_str": {"S": "bob"}}},
                        ]
                    },
                    {
                        "L": [
                            {"M": {"a_int": {"N": "789"}, "a_str": {"S": "cathy"}}},
                            {"M": {"a_int": {"N": "101112"}, "a_str": {"S": "david"}}},
                        ]
                    },
                ],
            },
            "a_struct_of_list": {
                "M": {
                    "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                    "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                }
            },
            "a_struct_of_struct_of_list": {
                "M": {
                    "struct_1": {
                        "M": {
                            "a_int_list1": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                            "a_str_list1": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                        }
                    },
                    "struct_2": {
                        "M": {
                            "a_int_list2": {"L": [{"N": "4"}, {"N": "5"}, {"N": "6"}]},
                            "a_str_list2": {"L": [{"S": "d"}, {"S": "e"}, {"S": "f"}]},
                        }
                    },
                }
            },
        },
        simple_schema={
            "a_list_of_struct": List(
                Struct(
                    {
                        "a_int": Integer(),
                        "a_str": String(),
                    }
                )
            ),
            "a_list_of_list_of_struct": List(
                List(
                    Struct(
                        {
                            "a_int": Integer(),
                            "a_str": String(),
                        }
                    )
                )
            ),
            "a_struct_of_list": Struct(
                {
                    "a_int_list": List(Integer()),
                    "a_str_list": List(String()),
                }
            ),
            "a_struct_of_struct_of_list": Struct(
                {
                    "struct_1": Struct(
                        {
                            "a_int_list1": List(Integer()),
                            "a_str_list1": List(String()),
                        }
                    ),
                    "struct_2": Struct(
                        {
                            "a_int_list2": List(Integer()),
                            "a_str_list2": List(String()),
                        }
                    ),
                }
            ),
        },
    )

    case12 = Case(
        item={
            "id": "id-1",
            "a_int": 123,
            "a_float": 3.14,
            "a_str": "alice",
            "a_binary": b"hello",
            "a_bool": False,
            "a_null": None,
            "a_int_set": [1, 2, 3],
            "a_float_set": [1.1, 2.2, 3.3],
            "a_str_set": ["a", "b", "c"],
            "a_binary_set": [b"hello", b"world"],
            "a_int_list": [1, 2, 3],
            "a_float_list": [1.1, 2.2, 3.3],
            "a_str_list": ["a", "b", "c"],
            "a_binary_list": [b"hello", b"world"],
            "a_struct": {
                "a_int": 123,
                "a_float": 3.14,
                "a_str": "alice",
                "a_binary": b"hello",
                "a_bool": False,
                "a_null": None,
            },
            "a_list_of_struct": [
                {"a_int": 123, "a_str": "alice"},
                {"a_int": 456, "a_str": "bob"},
            ],
            "a_list_of_list_of_struct": [
                [
                    {"a_int": 123, "a_str": "alice"},
                    {"a_int": 456, "a_str": "bob"},
                ],
                [
                    {"a_int": 789, "a_str": "cathy"},
                    {"a_int": 101112, "a_str": "david"},
                ],
            ],
            "a_struct_of_list": {
                "a_int_list": [1, 2, 3],
                "a_str_list": ["a", "b", "c"],
            },
            "a_struct_of_struct_of_list": {
                "struct_1": {
                    "a_int_list1": [1, 2, 3],
                    "a_str_list1": ["a", "b", "c"],
                },
                "struct_2": {
                    "a_int_list2": [4, 5, 6],
                    "a_str_list2": ["d", "e", "f"],
                },
            },
            "a_list_of_super_complicate_struct": [
                {
                    "id": "id-1",
                    "a_int": 123,
                    "a_float": 3.14,
                    "a_str": "alice",
                    "a_binary": b"hello",
                    "a_bool": False,
                    "a_null": None,
                    "a_int_set": [1, 2, 3],
                    "a_float_set": [1.1, 2.2, 3.3],
                    "a_str_set": ["a", "b", "c"],
                    "a_binary_set": [b"hello", b"world"],
                    "a_int_list": [1, 2, 3],
                    "a_float_list": [1.1, 2.2, 3.3],
                    "a_str_list": ["a", "b", "c"],
                    "a_binary_list": [b"hello", b"world"],
                    "a_struct": {
                        "a_int": 123,
                        "a_float": 3.14,
                        "a_str": "alice",
                        "a_binary": b"hello",
                        "a_bool": False,
                        "a_null": None,
                    },
                    "a_list_of_struct": [
                        {"a_int": 123, "a_str": "alice"},
                        {"a_int": 456, "a_str": "bob"},
                    ],
                    "a_list_of_list_of_struct": [
                        [
                            {"a_int": 123, "a_str": "alice"},
                            {"a_int": 456, "a_str": "bob"},
                        ],
                        [
                            {"a_int": 789, "a_str": "cathy"},
                            {"a_int": 101112, "a_str": "david"},
                        ],
                    ],
                    "a_struct_of_list": {
                        "a_int_list": [1, 2, 3],
                        "a_str_list": ["a", "b", "c"],
                    },
                    "a_struct_of_struct_of_list": {
                        "struct_1": {
                            "a_int_list1": [1, 2, 3],
                            "a_str_list1": ["a", "b", "c"],
                        },
                        "struct_2": {
                            "a_int_list2": [4, 5, 6],
                            "a_str_list2": ["d", "e", "f"],
                        },
                    },
                },
            ],
        },
        json={
            "id": {"S": "id-1"},
            "a_int": {"N": "123"},
            "a_float": {"N": "3.14"},
            "a_str": {"S": "alice"},
            "a_binary": {"B": "aGVsbG8="},
            "a_bool": {"BOOL": False},
            "a_null": {"NULL": True},
            "a_int_set": {"NS": ["1", "2", "3"]},
            "a_float_set": {"NS": ["1.1", "2.2", "3.3"]},
            "a_str_set": {"SS": ["a", "b", "c"]},
            "a_binary_set": {"BS": ["aGVsbG8=", "d29ybGQ="]},
            "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
            "a_float_list": {"L": [{"N": "1.1"}, {"N": "2.2"}, {"N": "3.3"}]},
            "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
            "a_binary_list": {"L": [{"B": "aGVsbG8="}, {"B": "d29ybGQ="}]},
            "a_struct": {
                "M": {
                    "a_int": {"N": "123"},
                    "a_float": {"N": "3.14"},
                    "a_str": {"S": "alice"},
                    "a_binary": {"B": "aGVsbG8="},
                    "a_bool": {"BOOL": False},
                    "a_null": {"NULL": True},
                }
            },
            "a_list_of_struct": {
                "L": [
                    {"M": {"a_int": {"N": "123"}, "a_str": {"S": "alice"}}},
                    {"M": {"a_int": {"N": "456"}, "a_str": {"S": "bob"}}},
                ]
            },
            "a_list_of_list_of_struct": {
                "L": [
                    {
                        "L": [
                            {"M": {"a_int": {"N": "123"}, "a_str": {"S": "alice"}}},
                            {"M": {"a_int": {"N": "456"}, "a_str": {"S": "bob"}}},
                        ]
                    },
                    {
                        "L": [
                            {"M": {"a_int": {"N": "789"}, "a_str": {"S": "cathy"}}},
                            {"M": {"a_int": {"N": "101112"}, "a_str": {"S": "david"}}},
                        ]
                    },
                ],
            },
            "a_struct_of_list": {
                "M": {
                    "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                    "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                }
            },
            "a_struct_of_struct_of_list": {
                "M": {
                    "struct_1": {
                        "M": {
                            "a_int_list1": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                            "a_str_list1": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                        }
                    },
                    "struct_2": {
                        "M": {
                            "a_int_list2": {"L": [{"N": "4"}, {"N": "5"}, {"N": "6"}]},
                            "a_str_list2": {"L": [{"S": "d"}, {"S": "e"}, {"S": "f"}]},
                        }
                    },
                }
            },
            "a_list_of_super_complicate_struct": {
                "L": [
                    {
                        "M": {
                            "id": {"S": "id-1"},
                            "a_int": {"N": "123"},
                            "a_float": {"N": "3.14"},
                            "a_str": {"S": "alice"},
                            "a_binary": {"B": "aGVsbG8="},
                            "a_bool": {"BOOL": False},
                            "a_null": {"NULL": True},
                            "a_int_set": {"NS": ["1", "2", "3"]},
                            "a_float_set": {"NS": ["1.1", "2.2", "3.3"]},
                            "a_str_set": {"SS": ["a", "b", "c"]},
                            "a_binary_set": {"BS": ["aGVsbG8=", "d29ybGQ="]},
                            "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                            "a_float_list": {
                                "L": [{"N": "1.1"}, {"N": "2.2"}, {"N": "3.3"}]
                            },
                            "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                            "a_binary_list": {
                                "L": [{"B": "aGVsbG8="}, {"B": "d29ybGQ="}]
                            },
                            "a_struct": {
                                "M": {
                                    "a_int": {"N": "123"},
                                    "a_float": {"N": "3.14"},
                                    "a_str": {"S": "alice"},
                                    "a_binary": {"B": "aGVsbG8="},
                                    "a_bool": {"BOOL": False},
                                    "a_null": {"NULL": True},
                                }
                            },
                            "a_list_of_struct": {
                                "L": [
                                    {
                                        "M": {
                                            "a_int": {"N": "123"},
                                            "a_str": {"S": "alice"},
                                        }
                                    },
                                    {
                                        "M": {
                                            "a_int": {"N": "456"},
                                            "a_str": {"S": "bob"},
                                        }
                                    },
                                ]
                            },
                            "a_list_of_list_of_struct": {
                                "L": [
                                    {
                                        "L": [
                                            {
                                                "M": {
                                                    "a_int": {"N": "123"},
                                                    "a_str": {"S": "alice"},
                                                }
                                            },
                                            {
                                                "M": {
                                                    "a_int": {"N": "456"},
                                                    "a_str": {"S": "bob"},
                                                }
                                            },
                                        ]
                                    },
                                    {
                                        "L": [
                                            {
                                                "M": {
                                                    "a_int": {"N": "789"},
                                                    "a_str": {"S": "cathy"},
                                                }
                                            },
                                            {
                                                "M": {
                                                    "a_int": {"N": "101112"},
                                                    "a_str": {"S": "david"},
                                                }
                                            },
                                        ]
                                    },
                                ],
                            },
                            "a_struct_of_list": {
                                "M": {
                                    "a_int_list": {
                                        "L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]
                                    },
                                    "a_str_list": {
                                        "L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]
                                    },
                                }
                            },
                            "a_struct_of_struct_of_list": {
                                "M": {
                                    "struct_1": {
                                        "M": {
                                            "a_int_list1": {
                                                "L": [
                                                    {"N": "1"},
                                                    {"N": "2"},
                                                    {"N": "3"},
                                                ]
                                            },
                                            "a_str_list1": {
                                                "L": [
                                                    {"S": "a"},
                                                    {"S": "b"},
                                                    {"S": "c"},
                                                ]
                                            },
                                        }
                                    },
                                    "struct_2": {
                                        "M": {
                                            "a_int_list2": {
                                                "L": [
                                                    {"N": "4"},
                                                    {"N": "5"},
                                                    {"N": "6"},
                                                ]
                                            },
                                            "a_str_list2": {
                                                "L": [
                                                    {"S": "d"},
                                                    {"S": "e"},
                                                    {"S": "f"},
                                                ]
                                            },
                                        }
                                    },
                                }
                            },
                        }
                    }
                ]
            },
        },
        simple_schema={
            "id": String(),
            "a_str": String(),
            "a_int": Integer(),
            "a_float": Float(),
            "a_binary": Binary(),
            "a_bool": Bool(),
            "a_null": Null(),
            "a_str_set": Set(String()),
            "a_int_set": Set(Integer()),
            "a_float_set": Set(Float()),
            "a_binary_set": Set(Binary()),
            "a_str_list": List(String()),
            "a_int_list": List(Integer()),
            "a_float_list": List(Float()),
            "a_binary_list": List(Binary()),
            "a_struct": Struct(
                {
                    "a_str": String(),
                    "a_int": Integer(),
                    "a_float": Float(),
                    "a_binary": Binary(),
                    "a_bool": Bool(),
                    "a_null": Null(),
                }
            ),
            "a_list_of_struct": List(
                Struct(
                    {
                        "a_str": String(),
                        "a_int": Integer(),
                    }
                )
            ),
            "a_list_of_list_of_struct": List(
                List(
                    Struct(
                        {
                            "a_str": String(),
                            "a_int": Integer(),
                        }
                    )
                )
            ),
            "a_struct_of_list": Struct(
                {
                    "a_str_list": List(String()),
                    "a_int_list": List(Integer()),
                }
            ),
            "a_struct_of_struct_of_list": Struct(
                {
                    "struct_1": Struct(
                        {
                            "a_str_list1": List(String()),
                            "a_int_list1": List(Integer()),
                        }
                    ),
                    "struct_2": Struct(
                        {
                            "a_str_list2": List(String()),
                            "a_int_list2": List(Integer()),
                        }
                    ),
                }
            ),
            "a_list_of_super_complicate_struct": List(
                Struct(
                    {
                        "id": String(),
                        "a_str": String(),
                        "a_int": Integer(),
                        "a_float": Float(),
                        "a_binary": Binary(),
                        "a_bool": Bool(),
                        "a_null": Null(),
                        "a_str_set": Set(String()),
                        "a_int_set": Set(Integer()),
                        "a_float_set": Set(Float()),
                        "a_binary_set": Set(Binary()),
                        "a_str_list": List(String()),
                        "a_int_list": List(Integer()),
                        "a_float_list": List(Float()),
                        "a_binary_list": List(Binary()),
                        "a_struct": Struct(
                            {
                                "a_str": String(),
                                "a_int": Integer(),
                                "a_float": Float(),
                                "a_binary": Binary(),
                                "a_bool": Bool(),
                                "a_null": Null(),
                            }
                        ),
                        "a_list_of_struct": List(
                            Struct(
                                {
                                    "a_str": String(),
                                    "a_int": Integer(),
                                }
                            )
                        ),
                        "a_list_of_list_of_struct": List(
                            List(
                                Struct(
                                    {
                                        "a_str": String(),
                                        "a_int": Integer(),
                                    }
                                )
                            )
                        ),
                        "a_struct_of_list": Struct(
                            {
                                "a_str_list": List(String()),
                                "a_int_list": List(Integer()),
                            }
                        ),
                        "a_struct_of_struct_of_list": Struct(
                            {
                                "struct_1": Struct(
                                    {
                                        "a_str_list1": List(String()),
                                        "a_int_list1": List(Integer()),
                                    }
                                ),
                                "struct_2": Struct(
                                    {
                                        "a_str_list2": List(String()),
                                        "a_int_list2": List(Integer()),
                                    }
                                ),
                            }
                        ),
                    }
                )
            ),
        },
    )

    # --------------------------------------------------------------------------
    # Serialize
    # --------------------------------------------------------------------------
    case101 = Case(
        item={
            "a_int": 1,
            "a_float": 3.14,
            "a_str": "Alice",
            "a_bin": b"hello",
            "a_bool": False,
            "a_null": None,
        },
        json={
            "a_int": {"N": "1"},
            "a_float": {"N": "3.14"},
            "a_str": {"S": "Alice"},
            "a_bin": {"B": "aGVsbG8="},
            "a_bool": {"BOOL": False},
            "a_null": {"NULL": True},
        },
        simple_schema={
            "a_int": Integer(default_for_null=-999),
            "a_float": Float(default_for_null=-999.999),
            "a_str": String(default_for_null="NA"),
            "a_bin": Binary(default_for_null=b"NA"),
            "a_bool": Bool(default_for_null=False),
            "a_null": Null(),
        },
    )

    case102 = Case(
        item={
            "a_int": None,
            "a_float": None,
            "a_str": None,
            "a_bin": None,
            "a_bool": None,
            "a_null": None,
        },
        json={
            "a_int": {"N": "-999"},
            "a_float": {"N": "-999.999"},
            "a_str": {"S": "NA"},
            "a_bin": {"B": "TkE="},
            "a_bool": {"BOOL": False},
            "a_null": {"NULL": True},
        },
        simple_schema={
            "a_int": Integer(default_for_null=-999),
            "a_float": Float(default_for_null=-999.999),
            "a_str": String(default_for_null="NA"),
            "a_bin": Binary(default_for_null=b"NA"),
            "a_bool": Bool(default_for_null=False),
            "a_null": Null(),
        },
    )

    case103 = Case(
        item={
            "a_struct": {
                "a_int": 1,
                "a_float": 3.14,
                "a_str": "Alice",
                "a_bin": b"hello",
                "a_bool": False,
                "a_null": None,
            },
        },
        json={
            "a_struct": {
                "M": {
                    "a_int": {"N": "1"},
                    "a_float": {"N": "3.14"},
                    "a_str": {"S": "Alice"},
                    "a_bin": {"B": "aGVsbG8="},
                    "a_bool": {"BOOL": False},
                    "a_null": {"NULL": True},
                },
            }
        },
        simple_schema={
            "a_struct": Struct(
                {
                    "a_int": Integer(default_for_null=-999),
                    "a_float": Float(default_for_null=-999.999),
                    "a_str": String(default_for_null="NA"),
                    "a_bin": Binary(default_for_null=b"NA"),
                    "a_bool": Bool(default_for_null=False),
                    "a_null": Null(),
                }
            ),
        },
    )

    case104 = Case(
        item={
            "a_struct": {
                "a_int": None,
                "a_float": None,
                "a_str": None,
                "a_bin": None,
                "a_bool": None,
                "a_null": None,
            },
        },
        json={
            "a_struct": {
                "M": {
                    "a_int": {"N": "-999"},
                    "a_float": {"N": "-999.999"},
                    "a_str": {"S": "NA"},
                    "a_bin": {"B": "TkE="},
                    "a_bool": {"BOOL": False},
                    "a_null": {"NULL": True},
                },
            }
        },
        simple_schema={
            "a_struct": Struct(
                {
                    "a_int": Integer(default_for_null=-999),
                    "a_float": Float(default_for_null=-999.999),
                    "a_str": String(default_for_null="NA"),
                    "a_bin": Binary(default_for_null=b"NA"),
                    "a_bool": Bool(default_for_null=False),
                    "a_null": Null(),
                }
            ),
        },
    )

    case105 = Case(
        item={
            "a_struct": None,
        },
        json={
            "a_struct": {
                "M": {
                    "a_int": {"N": "-999"},
                    "a_float": {"N": "-999.999"},
                    "a_str": {"S": "NA"},
                    "a_bin": {"B": "TkE="},
                    "a_bool": {"BOOL": False},
                    "a_null": {"NULL": True},
                },
            }
        },
        simple_schema={
            "a_struct": Struct(
                {
                    "a_int": Integer(default_for_null=-999),
                    "a_float": Float(default_for_null=-999.999),
                    "a_str": String(default_for_null="NA"),
                    "a_bin": Binary(default_for_null=b"NA"),
                    "a_bool": Bool(default_for_null=False),
                    "a_null": Null(),
                }
            ),
        },
    )

    case106 = Case(
        item={
            "a_int_set": [1, 2, 3],
            "a_float_set": [1.1, 2.2, 3.3],
            "a_str_set": ["a", "b", "c"],
            "a_binary_set": [b"hello", b"world"],
            "a_int_list": [1, 2, 3],
            "a_float_list": [1.1, 2.2, 3.3],
            "a_str_list": ["a", "b", "c"],
            "a_binary_list": [b"hello", b"world"],
            "a_bool_list": [True, False],
            "a_null_list": [None, None],
        },
        json={
            "a_int_set": {"NS": ["1", "2", "3"]},
            "a_float_set": {"NS": ["1.1", "2.2", "3.3"]},
            "a_str_set": {"SS": ["a", "b", "c"]},
            "a_binary_set": {"BS": ["aGVsbG8=", "d29ybGQ="]},
            "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
            "a_float_list": {"L": [{"N": "1.1"}, {"N": "2.2"}, {"N": "3.3"}]},
            "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
            "a_binary_list": {"L": [{"B": "aGVsbG8="}, {"B": "d29ybGQ="}]},
            "a_bool_list": {"L": [{"BOOL": True}, {"BOOL": False}]},
            "a_null_list": {"L": [{"NULL": True}, {"NULL": True}]},
        },
        simple_schema={
            "a_int_set": Set(Integer(default_for_null=-999)),
            "a_float_set": Set(Float(default_for_null=-999.999)),
            "a_str_set": Set(String(default_for_null="NA")),
            "a_binary_set": Set(Binary(default_for_null=b"NA")),
            "a_int_list": List(Integer(default_for_null=-999)),
            "a_float_list": List(Float(default_for_null=-999.999)),
            "a_str_list": List(String(default_for_null="NA")),
            "a_binary_list": List(Binary(default_for_null=b"NA")),
            "a_bool_list": List(Bool(default_for_null=False)),
            "a_null_list": List(Null(default_for_null=None)),
        },
    )

    case107 = Case(
        item={
            "a_int_set": [1, None, 3],
            "a_float_set": [1.1, None, 3.3],
            "a_str_set": [None, "b", "c"],
            "a_binary_set": [None, b"world"],
            "a_int_list": [1, None, 3],
            "a_float_list": [1.1, None, 3.3],
            "a_str_list": [None, "b", "c"],
            "a_binary_list": [None, b"world"],
            "a_bool_list": [None, False],
            "a_null_list": [None, None],
        },
        json={
            "a_int_set": {"NS": ["1", "-999", "3"]},
            "a_float_set": {"NS": ["1.1", "-999.999", "3.3"]},
            "a_str_set": {"SS": ["NA", "b", "c"]},
            "a_binary_set": {"BS": ["TkE=", "d29ybGQ="]},
            "a_int_list": {"L": [{"N": "1"}, {"N": "-999"}, {"N": "3"}]},
            "a_float_list": {"L": [{"N": "1.1"}, {"N": "-999.999"}, {"N": "3.3"}]},
            "a_str_list": {"L": [{"S": "NA"}, {"S": "b"}, {"S": "c"}]},
            "a_binary_list": {"L": [{"B": "TkE="}, {"B": "d29ybGQ="}]},
            "a_bool_list": {"L": [{"BOOL": False}, {"BOOL": False}]},
            "a_null_list": {"L": [{"NULL": True}, {"NULL": True}]},
        },
        simple_schema={
            "a_int_set": Set(Integer(default_for_null=-999)),
            "a_float_set": Set(Float(default_for_null=-999.999)),
            "a_str_set": Set(String(default_for_null="NA")),
            "a_binary_set": Set(Binary(default_for_null=b"NA")),
            "a_int_list": List(Integer(default_for_null=-999)),
            "a_float_list": List(Float(default_for_null=-999.999)),
            "a_str_list": List(String(default_for_null="NA")),
            "a_binary_list": List(Binary(default_for_null=b"NA")),
            "a_bool_list": List(Bool(default_for_null=False)),
            "a_null_list": List(Null(default_for_null=None)),
        },
    )

    case108 = Case(
        item={
            "a_int_set": None,
            "a_float_set": None,
            "a_str_set": None,
            "a_binary_set": None,
            "a_int_list": None,
            "a_float_list": None,
            "a_str_list": None,
            "a_binary_list": None,
            "a_bool_list": None,
            "a_null_list": None,
        },
        json={
            "a_int_set": {"NS": []},
            "a_float_set": {"NS": []},
            "a_str_set": {"SS": []},
            "a_binary_set": {"BS": []},
            "a_int_list": {"L": []},
            "a_float_list": {"L": []},
            "a_str_list": {"L": []},
            "a_binary_list": {"L": []},
            "a_bool_list": {"L": []},
            "a_null_list": {"L": []},
        },
        simple_schema={
            "a_int_set": Set(Integer(default_for_null=-999)),
            "a_float_set": Set(Float(default_for_null=-999.999)),
            "a_str_set": Set(String(default_for_null="NA")),
            "a_binary_set": Set(Binary(default_for_null=b"NA")),
            "a_int_list": List(Integer(default_for_null=-999)),
            "a_float_list": List(Float(default_for_null=-999.999)),
            "a_str_list": List(String(default_for_null="NA")),
            "a_binary_list": List(Binary(default_for_null=b"NA")),
            "a_bool_list": List(Bool(default_for_null=False)),
            "a_null_list": List(Null(default_for_null=None)),
        },
    )

    case109 = Case(
        item={
            "a_list_of_struct": [
                {"a_int": 123, "a_str": "alice"},
                {"a_int": 456, "a_str": "bob"},
            ],
            "a_list_of_list_of_struct": [
                [
                    {"a_int": 123, "a_str": "alice"},
                    {"a_int": 456, "a_str": "bob"},
                ],
                [
                    {"a_int": 789, "a_str": "cathy"},
                    {"a_int": 101112, "a_str": "david"},
                ],
            ],
            "a_struct_of_list": {
                "a_int_list": [1, 2, 3],
                "a_str_list": ["a", "b", "c"],
            },
            "a_struct_of_struct_of_list": {
                "struct_1": {
                    "a_int_list1": [1, 2, 3],
                    "a_str_list1": ["a", "b", "c"],
                },
                "struct_2": {
                    "a_int_list2": [4, 5, 6],
                    "a_str_list2": ["d", "e", "f"],
                },
            },
        },
        json={
            "a_list_of_struct": {
                "L": [
                    {"M": {"a_int": {"N": "123"}, "a_str": {"S": "alice"}}},
                    {"M": {"a_int": {"N": "456"}, "a_str": {"S": "bob"}}},
                ]
            },
            "a_list_of_list_of_struct": {
                "L": [
                    {
                        "L": [
                            {"M": {"a_int": {"N": "123"}, "a_str": {"S": "alice"}}},
                            {"M": {"a_int": {"N": "456"}, "a_str": {"S": "bob"}}},
                        ]
                    },
                    {
                        "L": [
                            {"M": {"a_int": {"N": "789"}, "a_str": {"S": "cathy"}}},
                            {"M": {"a_int": {"N": "101112"}, "a_str": {"S": "david"}}},
                        ]
                    },
                ],
            },
            "a_struct_of_list": {
                "M": {
                    "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                    "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                }
            },
            "a_struct_of_struct_of_list": {
                "M": {
                    "struct_1": {
                        "M": {
                            "a_int_list1": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                            "a_str_list1": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                        }
                    },
                    "struct_2": {
                        "M": {
                            "a_int_list2": {"L": [{"N": "4"}, {"N": "5"}, {"N": "6"}]},
                            "a_str_list2": {"L": [{"S": "d"}, {"S": "e"}, {"S": "f"}]},
                        }
                    },
                }
            },
        },
        simple_schema={
            "a_list_of_struct": List(
                Struct(
                    {
                        "a_int": Integer(default_for_null=-999),
                        "a_str": String(default_for_null="NA"),
                    }
                )
            ),
            "a_list_of_list_of_struct": List(
                List(
                    Struct(
                        {
                            "a_int": Integer(default_for_null=-999),
                            "a_str": String(default_for_null="NA"),
                        }
                    )
                )
            ),
            "a_struct_of_list": Struct(
                {
                    "a_int_list": List(Integer(default_for_null=-999)),
                    "a_str_list": List(String(default_for_null="NA")),
                }
            ),
            "a_struct_of_struct_of_list": Struct(
                {
                    "struct_1": Struct(
                        {
                            "a_int_list1": List(Integer(default_for_null=-999)),
                            "a_str_list1": List(String(default_for_null="NA")),
                        }
                    ),
                    "struct_2": Struct(
                        {
                            "a_int_list2": List(Integer(default_for_null=-999)),
                            "a_str_list2": List(String(default_for_null="NA")),
                        }
                    ),
                }
            ),
        },
    )

    @classmethod
    def items(cls) -> T.Iterable[T.Tuple[str, Case]]:
        for k, v in cls.__dict__.items():
            if isinstance(v, Case):
                yield k, v
