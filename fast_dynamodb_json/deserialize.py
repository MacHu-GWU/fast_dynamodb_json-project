# -*- coding: utf-8 -*-

"""
See :func:`deserialize` and :func:`deserialize_df` for more details.
"""

import typing as T
import polars as pl

from .typehint import (
    T_ITEM,
    T_JSON,
    T_SIMPLE_SCHEMA,
)
from .schema import (
    DATA_TYPE,
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


def _get_selector(
    name: T.Optional[str],
    dtype: DATA_TYPE,
    node: T.Optional["pl.Expr"] = pl.col("Item"),
    is_set: bool = False,
    is_list: bool = False,
) -> T.Optional["pl.Expr"]:
    """
    Get a polars expression for a given field that is used to deserialize
    regular Python dict from DynamoDB json data.
    """
    # print(f"{name = }") # for debug only
    # print(f"{dtype = }") # for debug only
    # print(f"{node = }") # for debug only
    # print(f"{is_set = }") # for debug only
    # print(f"{is_list = }") # for debug only

    # fmt: off
    if isinstance(dtype, Integer):
        if is_set:
            return pl.element().cast(pl.Int64)
        elif is_list:
            return node.struct.field("N").cast(pl.Int64)
        else:
            return node.struct.field("N").cast(pl.Int64).alias(name)
    elif isinstance(dtype, Float):
        if is_set:
            return pl.element().cast(pl.Float64)
        elif is_list:
            return node.struct.field("N").cast(pl.Float64)
        else:
            return node.struct.field("N").cast(pl.Float64).alias(name)
    elif isinstance(dtype, String):
        if is_set:
            return pl.element()
        elif is_list:
            return node.struct.field("S")
        else:
            return node.struct.field("S").alias(name)
    elif isinstance(dtype, Binary):
        if is_set:
            return pl.element().cast(pl.Binary).bin.decode("base64")
        elif is_list:
            return node.struct.field("B").cast(pl.Binary).bin.decode("base64")
        else:
            return node.struct.field("B").cast(pl.Binary).bin.decode("base64").alias(name)
    elif isinstance(dtype, Bool):
        return node.struct.field("BOOL").alias(name)
    elif isinstance(dtype, Null):
        return pl.lit(None).alias(name)

    # --------------------------------------------------------------------------
    # Set
    # --------------------------------------------------------------------------
    elif isinstance(dtype, Set):
        if isinstance(dtype.itype, String):
            field = "SS"
        elif isinstance(dtype.itype, Integer):
            field = "NS"
        elif isinstance(dtype.itype, Float):
            field = "NS"
        elif isinstance(dtype.itype, Binary):
            field = "BS"
        else:
            raise NotImplementedError
        expr = _get_selector(name=None, dtype=dtype.itype, node=None, is_set=True)
        final_expr = node.struct.field(field).list.eval(expr)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr

    # --------------------------------------------------------------------------
    # List
    # --------------------------------------------------------------------------
    elif isinstance(dtype, List):
        expr = _get_selector(name=None, dtype=dtype.itype, node=pl.element(), is_list=True)
        final_expr = node.struct.field("L").list.eval(expr)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr

    # --------------------------------------------------------------------------
    # Struct
    # --------------------------------------------------------------------------
    elif isinstance(dtype, Struct):
        fields = list()
        # for field in t.types:
        for key, vtype in dtype.types.items():
            new_node = node.struct.field("M").struct.field(key)
            expr = _get_selector(name=key, dtype=vtype, node=new_node)
            fields.append(expr)
        final_expr = pl.struct(*fields)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr
    else: # pragma: no cover
        return None


def deserialize_df(
    df: pl.DataFrame,
    simple_schema: T_SIMPLE_SCHEMA,
    dynamodb_json_col: str = "Item",
) -> pl.DataFrame:
    """
    similar to :func:`deserialize`, but work with polars DataFrame.

    :param df: polars DataFrame with a column of DynamoDB json data. Sample dataframe::

        import polars as pl

        df = pl.DataFrame(
            {
                "Item": [
                    {
                        "pk": {"S": "pk1"},
                        "sk": {"S": "sk1"},
                        "a_list" {
                            "L": [
                                {"S": "hello"},
                                {"S": "world"},
                            ],
                        },
                        "a_dict": {
                            "M": {
                                "a": {"N": "1"},
                                "b": {"N": "2"},
                            }
                        }
                    },
                    ...
                ],
            },
        )

    :param simple_schema: Schema of the data.
    :param dynamodb_json_col: Name of the column that contains DynamoDB json data.
        for example: "Item".

    :return: polars DataFrame with columns of the data. Sample dataframe::

        +-----+-----+--------------------+------------------+
        |  pk |  sk |       a_list       |      a_dict      |
        +-----+-----+--------------------+------------------+
        | pk1 | sk1 | ["hello", "world"] | {"a": 1, "b": 2} |
        +-----+-----+--------------------+------------------+
        |     |     |                    |                  |
        +-----+-----+--------------------+------------------+
    """
    selectors = []
    for name, dtype in simple_schema.items():
        # print(f"--- expr of field({name!r}) ---")
        selector = _get_selector(
            name,
            dtype=dtype,
            node=pl.col(dynamodb_json_col).struct.field(name),
        )
        # print(selector)
        if selector is not None:
            selectors.append(selector)
    return df.with_columns(*selectors).drop(dynamodb_json_col)


def deserialize(
    records: T.Iterable[T_ITEM],
    simple_schema: T_SIMPLE_SCHEMA,
) -> T.List[T_JSON]:
    """
    Convert DynamoDB json dict into regular Python dict.

    :param records: List of regular Python dict data. Example::

        records = [
            {
                "pk": {"S": "pk1"},
                "sk": {"S": "sk1"},
                "a_list" {
                    "L": [
                        {"S": "hello"},
                        {"S": "world"},
                    ],
                },
                "a_dict": {
                    "M": {
                        "a": {"N": "1"},
                        "b": {"N": "2"},
                    }
                }
            },
            ...
        ]

    :param simple_schema: Schema of the data. Example::

        simple_schema = {
            "pk": String(),
            "sk": String(),
            "a_list": List(String()),
            "a_dict": Struct({
                "a": Integer(),
                "b": Integer(),
            }),
        }

    :return: List of python dict data. Example::

        result = [
            {
                "pk": "pk1",
                "sk": "sk1",
                "a_list": ["hello", "world"],
                "a_dict": {
                    "a": 1,
                    "b": 2,
                },
            },
            ...
        ]
    """
    tmp_col = "Item"
    dynamodb_json_polars_schema = {
        k: vtype.to_dynamodb_json_polars() for k, vtype in simple_schema.items()
    }
    # print(f"{dynamodb_json_polars_schema = }") # for debug only
    df = pl.DataFrame(
        [{tmp_col: record} for record in records],
        schema={tmp_col: pl.Struct(dynamodb_json_polars_schema)},
        strict=False,
    )
    # print(df.to_dicts()) # for debug only
    df = deserialize_df(df=df, simple_schema=simple_schema, dynamodb_json_col=tmp_col)
    return df.to_dicts()
