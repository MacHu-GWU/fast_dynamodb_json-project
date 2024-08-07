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
    assert Struct(
        {
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
            "a_bin": Binary(),
            "a_bool": Bool(),
            "a_null": Null(),
        }
    ).to_polars() == pl.Struct(
        {
            "a_int": pl.Int64(),
            "a_float": pl.Float64(),
            "a_str": pl.Utf8(),
            "a_bin": pl.Binary(),
            "a_bool": pl.Boolean(),
            "a_null": pl.Null(),
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
    assert Set(Bool()).to_polars() == pl.List(pl.Boolean())
    assert Set(Null()).to_polars() == pl.List(pl.Null())

    # fmt: off
    assert List(List(Integer())).to_polars() == pl.List(pl.List(pl.Int64()))
    assert List(List(List(Integer()))).to_polars() == pl.List(pl.List(pl.List(pl.Int64())))
    # fmt: on

    assert Struct(
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
    ).to_polars() == pl.Struct(
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


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_cov_test

    run_cov_test(__file__, "fast_dynamodb_json.schema", preview=False)
