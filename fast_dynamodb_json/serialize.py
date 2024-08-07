# -*- coding: utf-8 -*-

"""
See :func:`serialize` and :func:`serialize_df` for more details.
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


def get_selector(
    name: T.Optional[str],
    dtype: DATA_TYPE,
    node: T.Optional["pl.Expr"] = pl.col("Data"),
    is_set: bool = False,
    is_list: bool = False,
) -> T.Optional[pl.Expr]:
    """
    Get a polars expression for a given field that is used to serialize
    regular Python dict into DynamoDB json data.
    """
    # print(f"{name = }") # for debug only
    # print(f"{dtype = }") # for debug only
    # print(f"{node = }") # for debug only
    # print(f"{is_set = }") # for debug only
    # print(f"{is_list = }") # for debug only

    # fmt: off
    if isinstance(dtype, Integer):
        if is_set:
            return pl.element().fill_null(dtype.default_for_null).cast(pl.Utf8())
        elif is_list:
            return pl.struct(
                pl.element().fill_null(dtype.default_for_null).cast(pl.Utf8()).alias("N")
            )
        else:
            return pl.struct(
                node.fill_null(dtype.default_for_null).cast(pl.Utf8).alias("N")
            ).alias(name)
    elif isinstance(dtype, Float):
        if is_set:
            return pl.element().fill_null(dtype.default_for_null).cast(pl.Utf8())
        elif is_list:
            return pl.struct(
                pl.element().fill_null(dtype.default_for_null).cast(pl.Utf8()).alias("N")
            )
        else:
            return pl.struct(
                node.fill_null(pl.lit(dtype.default_for_null)).cast(pl.Utf8).alias("N")
            ).alias(name)
    elif isinstance(dtype, String):
        if is_set:
            return pl.element().fill_null(dtype.default_for_null)
        elif is_list:
            return pl.struct(
                pl.element().fill_null(dtype.default_for_null).alias("S")
            )
        else:
            return pl.struct(
                node.fill_null(pl.lit(dtype.default_for_null)).alias("S")
            ).alias(name)
    elif isinstance(dtype, Binary):
        if is_set:
            return pl.element().fill_null(dtype.default_for_null).bin.encode("base64").cast(pl.Utf8())
        elif is_list:
            return pl.struct(
                pl.element().fill_null(dtype.default_for_null).bin.encode("base64").cast(pl.Utf8).alias("B")
            )
        else:
            return pl.struct(
                node.fill_null(pl.lit(dtype.default_for_null)).bin.encode("base64").cast(pl.Utf8).alias("B")
            ).alias(name)
    elif isinstance(dtype, Bool):
        if is_list:
            return pl.struct(
                pl.element().fill_null(dtype.default_for_null).alias("BOOL")
            )
        else:
            return pl.struct(
                node.fill_null(pl.lit(dtype.default_for_null)).alias("BOOL")
            ).alias(name)
    elif isinstance(dtype, Null):
        if is_list:
            return pl.struct(
                pl.element().fill_null(True).alias("NULL")
            )
        else:
            return pl.struct(
                pl.lit(True).alias("NULL")
            ).alias(name)

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
        else:# pragma: no cover
            raise NotImplementedError
        expr = get_selector(name=None, dtype=dtype.itype, node=pl.element(), is_set=True)
        final_expr = pl.struct(
            node.fill_null(dtype.default_for_null).list.eval(expr).alias(field)
        )
        if name:
            final_expr = final_expr.alias(name)
        return final_expr

    # --------------------------------------------------------------------------
    # List
    # --------------------------------------------------------------------------
    elif isinstance(dtype, List):
        expr = get_selector(name=None, dtype=dtype.itype, node=pl.element(), is_list=True)
        final_expr = pl.struct(
            node.fill_null(dtype.default_for_null).list.eval(expr).alias("L")
        )
        if name:
            final_expr = final_expr.alias(name)
        return final_expr

    # --------------------------------------------------------------------------
    # Struct
    # --------------------------------------------------------------------------
    elif isinstance(dtype, Struct):
        fields = list()
        for key, vtype in dtype.types.items():
            new_node = node.struct.field(key)
            expr = get_selector(name=key, dtype=vtype, node=new_node)
            fields.append(expr)
        final_expr = pl.struct(pl.struct(*fields).alias("M"))
        if name:
            final_expr = final_expr.alias(name)
        return final_expr
    else: # pragma: no cover
        return None
    # fmt: on


def serialize_df(
    df: pl.DataFrame,
    simple_schema: T_SIMPLE_SCHEMA,
    data_col: str = "Data",
) -> pl.DataFrame:
    """
    similar to :func:`serialize`, but work with polars DataFrame.

    :param df: polars DataFrame with a column of DynamoDB json data. Sample dataframe::

        import polars as pl

        df = pl.DataFrame(
            {
                "Data": [
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
                ],
            },
        )

    :param simple_schema: Schema of the data.
    :param dynamodb_json_col: Name of the column that contains regular Python dict data.
        for example: "Data".

    :return: polars DataFrame with columns of the DynamoDB JSON data. Sample dataframe::

        +--------------+--------------+-----------------------------------------+-------------------------------------------+
        |      pk      |      sk      |                  a_list                 |                   a_dict                  |
        +--------------+--------------+-----------------------------------------+-------------------------------------------+
        | {"S": "pk1"} | {"S": "sk1"} | {"L": [{"S": "hello"}, {"S": "world"}]} | {"M": {"a": {"N": "1"}, "b": {"N": "2"}}} |
        +--------------+--------------+-----------------------------------------+-------------------------------------------+
        |              |              |                                         |                                           |
        +--------------+--------------+-----------------------------------------+-------------------------------------------+
    """
    selectors = []

    for name, dtype in simple_schema.items():
        # print(f"--- expr of field({name!r}) ---")
        selector = get_selector(
            name=name,
            dtype=dtype,
            node=pl.col(data_col).struct.field(name),
        )
        # print(selector)
        if selector is not None:
            selectors.append(selector)

    return df.with_columns(*selectors).drop(data_col)


def serialize(
    records: T.Iterable[T_ITEM],
    simple_schema: T_SIMPLE_SCHEMA,
) -> T.List[T_JSON]:
    """
    Convert regular Python dict data to DynamoDB JSON dict.

    :param records: List of regular Python dict data. Example::

        records = [
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

    :return: List of DynamoDB JSON data. Example::

        result = [
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
    """
    data_col = "Data"
    polars_schema = {k: vtype.to_polars() for k, vtype in simple_schema.items()}
    # print(f"{polars_schema = }") # for debug only
    df = pl.DataFrame(
        [{data_col: record} for record in records],
        schema={data_col: pl.Struct(polars_schema)},
        strict=False,
    )
    # print(df.to_dicts()) # for debug only
    df = serialize_df(df=df, simple_schema=simple_schema, data_col=data_col)
    return df.to_dicts()
