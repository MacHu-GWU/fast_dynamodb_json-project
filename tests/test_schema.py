# -*- coding: utf-8 -*-

import polars as pl
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


def test():
    simple_type = Struct(
        {
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
            "a_bin": Binary(),
            "a_bool": Bool(),
            "a_null": Null(),
        }
    )
    assert simple_type.to_polars() == pl.Struct(
        {
            "a_int": pl.Int64(),
            "a_float": pl.Float64(),
            "a_str": pl.Utf8(),
            "a_bin": pl.Binary(),
            "a_bool": pl.Boolean(),
            "a_null": pl.Null(),
        }
    )
    assert simple_type.to_dynamodb_json_polars() == pl.Struct(
        {
            "M": pl.Struct(
                {
                    "a_int": pl.Struct({"N": pl.Utf8()}),
                    "a_float": pl.Struct({"N": pl.Utf8()}),
                    "a_str": pl.Struct({"S": pl.Utf8()}),
                    "a_bin": pl.Struct({"B": pl.Utf8()}),
                    "a_bool": pl.Struct({"BOOL": pl.Boolean()}),
                    "a_null": pl.Struct({"NULL": pl.Boolean()}),
                }
            )
        }
    )

    assert List(Integer()).to_polars() == pl.List(pl.Int64())
    assert List(Float()).to_polars() == pl.List(pl.Float64())
    assert List(String()).to_polars() == pl.List(pl.Utf8())
    assert List(Binary()).to_polars() == pl.List(pl.Binary())
    assert List(Bool()).to_polars() == pl.List(pl.Boolean())
    assert List(Null()).to_polars() == pl.List(pl.Null())

    assert Set(Integer()).to_polars() == pl.List(pl.Int64())
    assert Set(Float()).to_polars() == pl.List(pl.Float64())
    assert Set(String()).to_polars() == pl.List(pl.Utf8())
    assert Set(Binary()).to_polars() == pl.List(pl.Binary())

    assert List(Integer()).to_dynamodb_json_polars() == pl.Struct(
        {"L": pl.List(pl.Struct({"N": pl.Utf8()}))}
    )
    assert List(Float()).to_dynamodb_json_polars() == pl.Struct(
        {"L": pl.List(pl.Struct({"N": pl.Utf8()}))}
    )
    assert List(String()).to_dynamodb_json_polars() == pl.Struct(
        {"L": pl.List(pl.Struct({"S": pl.Utf8()}))}
    )
    assert List(Binary()).to_dynamodb_json_polars() == pl.Struct(
        {"L": pl.List(pl.Struct({"B": pl.Utf8()}))}
    )
    assert List(Bool()).to_dynamodb_json_polars() == pl.Struct(
        {"L": pl.List(pl.Struct({"BOOL": pl.Boolean()}))}
    )
    assert List(Null()).to_dynamodb_json_polars() == pl.Struct(
        {"L": pl.List(pl.Struct({"NULL": pl.Boolean()}))}
    )

    assert Set(Integer()).to_dynamodb_json_polars() == pl.Struct(
        {"NS": pl.List(pl.Utf8())}
    )
    assert Set(Float()).to_dynamodb_json_polars() == pl.Struct(
        {"NS": pl.List(pl.Utf8())}
    )
    assert Set(String()).to_dynamodb_json_polars() == pl.Struct(
        {"SS": pl.List(pl.Utf8())}
    )
    assert Set(Binary()).to_dynamodb_json_polars() == pl.Struct(
        {"BS": pl.List(pl.Utf8())}
    )

    # fmt: off
    assert List(List(Integer())).to_polars() == pl.List(pl.List(pl.Int64()))
    assert List(List(List(Integer()))).to_polars() == pl.List(pl.List(pl.List(pl.Int64())))
    # fmt: on

    simple_type = Struct(
        {
            "a_list": List(Integer()),
            "a_set": Set(String()),
            "a_struct": Struct(
                {
                    "a_float": Float(),
                    "a_bin": Binary(),
                    "a_list": List(
                        Struct(
                            {
                                "a_bool": Bool(),
                                "a_null": Null(),
                            }
                        )
                    ),
                },
            ),
        },
    )
    assert simple_type.to_polars() == pl.Struct(
        {
            "a_list": pl.List(pl.Int64()),
            "a_set": pl.List(pl.Utf8()),
            "a_struct": pl.Struct(
                {
                    "a_float": pl.Float64(),
                    "a_bin": pl.Binary(),
                    "a_list": pl.List(
                        pl.Struct(
                            {
                                "a_bool": pl.Boolean(),
                                "a_null": pl.Null(),
                            },
                        )
                    ),
                },
            ),
        },
    )
    assert simple_type.to_dynamodb_json_polars() == pl.Struct(
        {
            "M": pl.Struct(
                {
                    "a_list": pl.Struct(
                        {
                            "L": pl.List(
                                pl.Struct({"N": pl.Utf8()}),
                            )
                        }
                    ),
                    "a_set": pl.Struct({"SS": pl.List(pl.Utf8())}),
                    "a_struct": pl.Struct(
                        {
                            "M": pl.Struct(
                                {
                                    "a_float": pl.Struct({"N": pl.Utf8()}),
                                    "a_bin": pl.Struct({"B": pl.Utf8()}),
                                    "a_list": pl.Struct(
                                        {
                                            "L": pl.List(
                                                pl.Struct(
                                                    {
                                                        "M": pl.Struct(
                                                            {
                                                                "a_bool": pl.Struct(
                                                                    {
                                                                        "BOOL": pl.Boolean()
                                                                    }
                                                                ),
                                                                "a_null": pl.Struct(
                                                                    {
                                                                        "NULL": pl.Boolean()
                                                                    }
                                                                ),
                                                            }
                                                        )
                                                    }
                                                ),
                                            )
                                        }
                                    ),
                                }
                            )
                        },
                    ),
                },
            )
        }
    )


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_cov_test

    run_cov_test(__file__, "fast_dynamodb_json.schema", preview=False)
