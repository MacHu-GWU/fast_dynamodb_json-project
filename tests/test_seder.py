# -*- coding: utf-8 -*-

import typing as T
from pathlib import Path

import polars as pl
from rich import print as rprint

from fast_dynamodb_json.schema import (
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
from fast_dynamodb_json.seder import deserialize

dir_here = Path(__file__).absolute().parent


def test():
    # --------------------------------------------------------------------------
    # DynamoDB json
    #
    # It is used in:
    # - put_item API request
    # - get_item API response
    # - import_table data
    # - export_table data
    # --------------------------------------------------------------------------
    dynamodb_json = {
        "id": {"S": "id-1"},
        "a_str": {"S": "alice"},
        "a_int": {"N": "123"},
        "a_float": {"N": "3.14"},
        "a_binary": {"B": "aGVsbG8="},
        "a_bool": {"BOOL": False},
        "a_null": {"NULL": True},
        "a_str_set": {"SS": ["a", "b", "c"]},
        "a_int_set": {"NS": ["1", "2", "3"]},
        "a_float_set": {"NS": ["1.1", "2.2", "3.3"]},
        "a_binary_set": {"BS": ["aGVsbG8=", "d29ybGQ="]},
        "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
        "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
        "a_float_list": {"L": [{"N": "1.1"}, {"N": "2.2"}, {"N": "3.3"}]},
        "a_binary_list": {"L": [{"B": "aGVsbG8="}, {"B": "d29ybGQ="}]},
        "a_struct": {
            "M": {
                "a_str": {"S": "alice"},
                "a_int": {"N": "123"},
                "a_float": {"N": "3.14"},
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
                "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
            }
        },
        "a_struct_of_struct_of_list": {
            "M": {
                "struct_1": {
                    "M": {
                        "a_str_list1": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                        "a_int_list1": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                    }
                },
                "struct_2": {
                    "M": {
                        "a_str_list2": {"L": [{"S": "d"}, {"S": "e"}, {"S": "f"}]},
                        "a_int_list2": {"L": [{"N": "4"}, {"N": "5"}, {"N": "6"}]},
                    }
                },
            }
        },
        "a_list_of_super_complicate_struct": {
            "L": [
                {
                    "M": {
                        "id": {"S": "id-1"},
                        "a_str": {"S": "alice"},
                        "a_int": {"N": "123"},
                        "a_float": {"N": "3.14"},
                        "a_binary": {"B": "aGVsbG8="},
                        "a_bool": {"BOOL": False},
                        "a_null": {"NULL": True},
                        "a_str_set": {"SS": ["a", "b", "c"]},
                        "a_int_set": {"NS": ["1", "2", "3"]},
                        "a_float_set": {"NS": ["1.1", "2.2", "3.3"]},
                        "a_binary_set": {"BS": ["aGVsbG8=", "d29ybGQ="]},
                        "a_str_list": {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]},
                        "a_int_list": {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]},
                        "a_float_list": {
                            "L": [{"N": "1.1"}, {"N": "2.2"}, {"N": "3.3"}]
                        },
                        "a_binary_list": {"L": [{"B": "aGVsbG8="}, {"B": "d29ybGQ="}]},
                        "a_struct": {
                            "M": {
                                "a_binary": {"B": "aGVsbG8="},
                                "a_bool": {"BOOL": False},
                                "a_float": {"N": "3.14"},
                                "a_int": {"N": "123"},
                                "a_null": {"NULL": True},
                                "a_str": {"S": "alice"},
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
                                "a_str_list": {
                                    "L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]
                                },
                                "a_int_list": {
                                    "L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]
                                },
                            }
                        },
                        "a_struct_of_struct_of_list": {
                            "M": {
                                "struct_1": {
                                    "M": {
                                        "a_str_list1": {
                                            "L": [
                                                {"S": "a"},
                                                {"S": "b"},
                                                {"S": "c"},
                                            ]
                                        },
                                        "a_int_list1": {
                                            "L": [
                                                {"N": "1"},
                                                {"N": "2"},
                                                {"N": "3"},
                                            ]
                                        },
                                    }
                                },
                                "struct_2": {
                                    "M": {
                                        "a_str_list2": {
                                            "L": [
                                                {"S": "d"},
                                                {"S": "e"},
                                                {"S": "f"},
                                            ]
                                        },
                                        "a_int_list2": {
                                            "L": [
                                                {"N": "4"},
                                                {"N": "5"},
                                                {"N": "6"},
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
    }

    # --------------------------------------------------------------------------
    # Developer friendly python dictionary
    # --------------------------------------------------------------------------
    item = {
        "id": "id-1",
        "a_str": "alice",
        "a_int": 123,
        "a_float": 3.14,
        "a_binary": b"hello",
        "a_bool": False,
        "a_null": None,
        "a_str_set": ["a", "b", "c"],
        "a_int_set": [1, 2, 3],
        "a_float_set": [1.1, 2.2, 3.3],
        "a_binary_set": [b"hello", b"world"],
        "a_str_list": ["a", "b", "c"],
        "a_int_list": [1, 2, 3],
        "a_float_list": [1.1, 2.2, 3.3],
        "a_binary_list": [b"hello", b"world"],
        "a_struct": {
            "a_str": "alice",
            "a_int": 123,
            "a_float": 3.14,
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
            "a_str_list": ["a", "b", "c"],
            "a_int_list": [1, 2, 3],
        },
        "a_struct_of_struct_of_list": {
            "struct_1": {
                "a_str_list1": ["a", "b", "c"],
                "a_int_list1": [1, 2, 3],
            },
            "struct_2": {
                "a_str_list2": ["d", "e", "f"],
                "a_int_list2": [4, 5, 6],
            },
        },
        "a_list_of_super_complicate_struct": [
            {
                "id": "id-1",
                "a_str": "alice",
                "a_int": 123,
                "a_float": 3.14,
                "a_binary": b"hello",
                "a_bool": False,
                "a_null": None,
                "a_str_set": ["a", "b", "c"],
                "a_int_set": [1, 2, 3],
                "a_float_set": [1.1, 2.2, 3.3],
                "a_binary_set": [b"hello", b"world"],
                "a_str_list": ["a", "b", "c"],
                "a_int_list": [1, 2, 3],
                "a_float_list": [1.1, 2.2, 3.3],
                "a_binary_list": [b"hello", b"world"],
                "a_struct": {
                    "a_str": "alice",
                    "a_int": 123,
                    "a_float": 3.14,
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
                    "a_str_list": ["a", "b", "c"],
                    "a_int_list": [1, 2, 3],
                },
                "a_struct_of_struct_of_list": {
                    "struct_1": {
                        "a_str_list1": ["a", "b", "c"],
                        "a_int_list1": [1, 2, 3],
                    },
                    "struct_2": {
                        "a_str_list2": ["d", "e", "f"],
                        "a_int_list2": [4, 5, 6],
                    },
                },
            },
        ],
    }

    # --------------------------------------------------------------------------
    # fast_dynamodb_json schema
    # --------------------------------------------------------------------------

    simple_schema = {
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
    }

    # --------------------------------------------------------------------------
    # polars DataFrame schema
    # --------------------------------------------------------------------------
    polars_schema = {
        "id": pl.Utf8(),
        "a_str": pl.Utf8(),
        "a_int": pl.Int64(),
        "a_float": pl.Float64(),
        "a_binary": pl.Binary(),
        "a_bool": pl.Boolean(),
        "a_null": pl.Null(),
        "a_str_set": pl.Array(pl.Utf8(), shape=1),
        "a_int_set": pl.Array(pl.Int64(), shape=1),
        "a_float_set": pl.Array(pl.Float64(), shape=1),
        "a_binary_set": pl.Array(pl.Binary(), shape=1),
        "a_str_list": pl.List(pl.Utf8()),
        "a_int_list": pl.List(pl.Int64()),
        "a_float_list": pl.List(pl.Float64()),
        "a_binary_list": pl.List(pl.Binary()),
        "a_struct": pl.Struct(
            {
                "a_str": pl.Utf8(),
                "a_int": pl.Int64(),
                "a_float": pl.Float64(),
                "a_binary": pl.Binary(),
                "a_bool": pl.Boolean(),
                "a_null": pl.Null(),
            }
        ),
        "a_list_of_struct": pl.List(
            pl.Struct(
                {
                    "a_str": pl.Utf8(),
                    "a_int": pl.Int64(),
                }
            )
        ),
        "a_list_of_list_of_struct": pl.List(
            pl.List(
                pl.Struct(
                    {
                        "a_str": pl.Utf8(),
                        "a_int": pl.Int64(),
                    }
                )
            )
        ),
        "a_struct_of_list": pl.Struct(
            {
                "a_str_list": pl.List(pl.Utf8()),
                "a_int_list": pl.List(pl.Int64()),
            }
        ),
        "a_struct_of_struct_of_list": pl.Struct(
            {
                "struct_1": pl.Struct(
                    {
                        "a_str_list1": pl.List(pl.Utf8()),
                        "a_int_list1": pl.List(pl.Int64()),
                    }
                ),
                "struct_2": pl.Struct(
                    {
                        "a_str_list2": pl.List(pl.Utf8()),
                        "a_int_list2": pl.List(pl.Int64()),
                    }
                ),
            }
        ),
        "a_list_of_super_complicate_struct": pl.List(
            pl.Struct(
                {
                    "id": pl.Utf8(),
                    "a_str": pl.Utf8(),
                    "a_int": pl.Int64(),
                    "a_float": pl.Float64(),
                    "a_binary": pl.Binary(),
                    "a_bool": pl.Boolean(),
                    "a_null": pl.Null(),
                    "a_str_set": pl.Array(pl.Utf8(), shape=1),
                    "a_int_set": pl.Array(pl.Int64(), shape=1),
                    "a_float_set": pl.Array(pl.Float64(), shape=1),
                    "a_binary_set": pl.Array(pl.Binary(), shape=1),
                    "a_str_list": pl.List(pl.Utf8()),
                    "a_int_list": pl.List(pl.Int64()),
                    "a_float_list": pl.List(pl.Float64()),
                    "a_binary_list": pl.List(pl.Binary()),
                    "a_struct": pl.Struct(
                        {
                            "a_str": pl.Utf8(),
                            "a_int": pl.Int64(),
                            "a_float": pl.Float64(),
                            "a_binary": pl.Binary(),
                            "a_bool": pl.Boolean(),
                            "a_null": pl.Null(),
                        }
                    ),
                    "a_list_of_struct": pl.List(
                        pl.Struct(
                            {
                                "a_str": pl.Utf8(),
                                "a_int": pl.Int64(),
                            }
                        )
                    ),
                    "a_list_of_list_of_struct": pl.List(
                        pl.List(
                            pl.Struct(
                                {
                                    "a_str": pl.Utf8(),
                                    "a_int": pl.Int64(),
                                }
                            )
                        )
                    ),
                    "a_struct_of_list": pl.Struct(
                        {
                            "a_str_list": pl.List(pl.Utf8()),
                            "a_int_list": pl.List(pl.Int64()),
                        }
                    ),
                    "a_struct_of_struct_of_list": pl.Struct(
                        {
                            "struct_1": pl.Struct(
                                {
                                    "a_str_list1": pl.List(pl.Utf8()),
                                    "a_int_list1": pl.List(pl.Int64()),
                                }
                            ),
                            "struct_2": pl.Struct(
                                {
                                    "a_str_list2": pl.List(pl.Utf8()),
                                    "a_int_list2": pl.List(pl.Int64()),
                                }
                            ),
                        }
                    ),
                }
            )
        ),
    }

    # --------------------------------------------------------------------------
    # The polars expression that can convert DynamoDB json to python dictionary
    # It should match generated polars expression
    #
    # For debugging, I usually first make this work, and then try to ensure that
    # this library can generate the same expression
    # --------------------------------------------------------------------------
    # fmt: off
    start = pl.col("Item")
    exprs = [
        start.struct.field("id").struct.field("S").alias("id"),
        start.struct.field("a_str").struct.field("S").alias("a_str"),
        start.struct.field("a_int").struct.field("N").cast(pl.Int64).alias("a_int"),
        start.struct.field("a_float").struct.field("N").cast(pl.Float64).alias("a_float"),
        start.struct.field("a_binary").struct.field("B").cast(pl.Binary).bin.decode("base64").alias("a_binary"),
        start.struct.field("a_bool").struct.field("BOOL").alias("a_bool"),
        pl.lit(None).alias("a_null"),
        start.struct.field("a_str_set").struct.field("SS").list.eval(  pl.element()  ).alias("a_str_set"),
        start.struct.field("a_int_set").struct.field("NS").list.eval(  pl.element().cast(pl.Int64)  ).alias("a_int_set"),
        start.struct.field("a_float_set").struct.field("NS").list.eval(  pl.element().cast(pl.Float64)  ).alias("a_float_set"),
        start.struct.field("a_binary_set").struct.field("BS").list.eval(  pl.element().cast(pl.Binary).bin.decode("base64")  ).alias("a_binary_set"),
        start.struct.field("a_str_list").struct.field("L").list.eval(  pl.element().struct.field("S")  ).alias("a_str_list"),
        start.struct.field("a_int_list").struct.field("L").list.eval(  pl.element().struct.field("N").cast(pl.Int64)  ).alias("a_int_list"),
        start.struct.field("a_float_list").struct.field("L").list.eval(  pl.element().struct.field("N").cast(pl.Float64)  ).alias("a_float_list"),
        start.struct.field("a_binary_list").struct.field("L").list.eval(  pl.element().struct.field("B").cast(pl.Binary).bin.decode("base64")  ).alias("a_binary_list"),
        pl.struct(
            start.struct.field("a_struct").struct.field("M").struct.field("a_str").struct.field("S").alias("a_str"),
            start.struct.field("a_struct").struct.field("M").struct.field("a_int").struct.field("N").cast(pl.Int64).alias("a_int"),
            start.struct.field("a_struct").struct.field("M").struct.field("a_float").struct.field("N").cast(pl.Float64).alias("a_float"),
            start.struct.field("a_struct").struct.field("M").struct.field("a_binary").struct.field("B").cast(pl.Binary).bin.decode("base64").alias("a_binary"),
            start.struct.field("a_struct").struct.field("M").struct.field("a_bool").struct.field("BOOL").alias("a_bool"),
            pl.lit(None).alias("a_null"),
        ).alias("a_struct"),
        start.struct.field("a_list_of_struct").struct.field("L").list.eval(
            pl.struct(
                pl.element().struct.field("M").struct.field("a_int").struct.field("N").alias("a_int"),
                pl.element().struct.field("M").struct.field("a_str").struct.field("S").alias("a_str"),
            ),
        ).alias("a_list_of_struct"),
        start.struct.field("a_list_of_list_of_struct").struct.field("L").list.eval(
            pl.element().struct.field("L").list.eval(
                pl.struct(
                    pl.element().struct.field("M").struct.field("a_int").struct.field("N").alias("a_int"),
                    pl.element().struct.field("M").struct.field("a_str").struct.field("S").alias("a_str"),
                ),
            )
        ).alias("a_list_of_list_of_struct"),
        pl.struct(
            start.struct.field("a_struct_of_list").struct.field("M").struct.field("a_str_list").struct.field("L").list.eval(pl.element().struct.field("S")).alias("a_str_list"),
            start.struct.field("a_struct_of_list").struct.field("M").struct.field("a_int_list").struct.field("L").list.eval(pl.element().struct.field("N").cast(pl.Int64)).alias("a_int_list"),
        ).alias("a_struct_of_list"),
        pl.struct(
            pl.struct(
                start.struct.field("a_struct_of_struct_of_list").struct.field("M").struct.field("struct_1").struct.field("M").struct.field("a_str_list1").struct.field("L").list.eval(pl.element().struct.field("S")).alias("a_str_list1"),
                start.struct.field("a_struct_of_struct_of_list").struct.field("M").struct.field("struct_1").struct.field("M").struct.field("a_int_list1").struct.field("L").list.eval(pl.element().struct.field("N").cast(pl.Int64)).alias("a_int_list1")
            ).alias("struct_1"),
            pl.struct(
                start.struct.field("a_struct_of_struct_of_list").struct.field("M").struct.field("struct_2").struct.field("M").struct.field("a_str_list2").struct.field("L").list.eval(pl.element().struct.field("S")).alias("a_str_list2"),
                start.struct.field("a_struct_of_struct_of_list").struct.field("M").struct.field("struct_2").struct.field("M").struct.field("a_int_list2").struct.field("L").list.eval(pl.element().struct.field("N").cast(pl.Int64)).alias("a_int_list2")
            ).alias("struct_2"),
        ).alias("a_struct_of_struct_of_list"),
    ]
    # fmt: on

    # --------------------------------------------------------------------------
    # Deserialize
    # --------------------------------------------------------------------------
    df = pl.DataFrame([{"Item": dynamodb_json}], strict=False)
    df = deserialize(df, simple_schema)
    # df = deserialize(df, schema)
    # df = df.with_columns(*exprs).drop("Item")


    rprint(df.to_dicts()[0])
    res = df.to_dicts()[0]
    assert res == item


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_cov_test

    run_cov_test(__file__, "fast_dynamodb_json.seder", preview=False)
