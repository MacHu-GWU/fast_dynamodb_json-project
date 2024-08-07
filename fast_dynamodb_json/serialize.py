# -*- coding: utf-8 -*-

import typing as T
import polars as pl

from .typehint import (
    T_ITEM,
    T_JSON,
    T_SIMPLE_SCHEMA,
    T_POLARS_SCHEMA,
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

if T.TYPE_CHECKING:  # pragma: no cover
    from polars.functions.col import Col


def get_selector(
    name: T.Optional[str],
    dtype: DATA_TYPE,
    node: T.Optional["pl.Expr"] = pl.col("Data"),
    is_set: bool = False,
    is_list: bool = False,
) -> T.Optional[pl.Expr]:
    # fmt: off
    if isinstance(dtype, Integer):
        if is_set:
            return pl.element().cast(pl.Int64)
        elif is_list:
            return node.struct.field("N").cast(pl.Int64)
        else:
            return pl.struct(node.fill_null(pl.lit(dtype.default_for_null)).cast(pl.Utf8).alias("N")).alias(name)
    elif isinstance(dtype, Float):
        if is_set:
            return pl.element().cast(pl.Float64)
        elif is_list:
            return node.struct.field("N").cast(pl.Float64)
        else:
            return pl.struct(node.fill_null(pl.lit(dtype.default_for_null)).cast(pl.Utf8).alias("N")).alias(name)
    elif isinstance(dtype, String):
        if is_set:
            return pl.element()
        elif is_list:
            return node.struct.field("S")
        else:
            return pl.struct(node.fill_null(pl.lit(dtype.default_for_null)).alias("S")).alias(name)
    elif isinstance(dtype, Binary):
        if is_set:
            return pl.element().cast(pl.Binary).bin.decode("base64")
        elif is_list:
            return node.struct.field("B").cast(pl.Binary).bin.decode("base64")
        else:
            return pl.struct(node.fill_null(pl.lit(dtype.default_for_null)).bin.encode("base64").cast(pl.Utf8).alias("B")).alias(name)
    elif isinstance(dtype, Bool):
        return pl.struct(node.fill_null(pl.lit(dtype.default_for_null)).alias("BOOL")).alias(name)
    elif isinstance(dtype, Null):
        return pl.struct(pl.lit(True).alias("NULL")).alias(name)

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
        expr = get_selector(name=None, dtype=dtype.itype, node=None, is_set=True)
        final_expr = node.struct.field(field).list.eval(expr)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr

    # --------------------------------------------------------------------------
    # List
    # --------------------------------------------------------------------------
    elif isinstance(dtype, List):
        expr = get_selector(name=None, dtype=dtype.itype, node=pl.element(), is_list=True)
        final_expr = pl.struct(node.fill_null(pl.lit(dtype.default_for_null)).list.eval(expr)).alias("L")
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
    else:
        return None

    # fmt: on


def serialize_df(
    df: pl.DataFrame,
    simple_schema: T_SIMPLE_SCHEMA,
    data_col: str = "Data",
) -> pl.DataFrame:
    selectors = []
    names = []

    exprs = []
    for name, dtype in simple_schema.items():
        if isinstance(dtype, (Integer, Float, String, Binary, Bool, Null)):
            expr = pl.col(data_col).struct.field(name).fill_null(value=pl.lit(dtype.default_for_null))
            exprs.append(expr)
        else:
            pass

    if len(exprs):
        df = df.with_columns(
            pl.struct(
                *exprs
            ).alias(data_col)
        )

    for name, dtype in simple_schema.items():
        selector = get_selector(
            name=name,
            dtype=dtype,
            node=pl.col(data_col).struct.field(name),
        )
        if selector is not None:
            selectors.append(selector)
            names.append(name)

    for name, selector in zip(names, selectors):
        print(f"--- expr of field({name!r}) ---")
        print(selector)

    return df.with_columns(*selectors).drop(data_col)


def serialize(
    records: T.Iterable[T_ITEM],
    simple_schema: T_SIMPLE_SCHEMA,
) -> T.List[T_JSON]:
    """
    Convert regular Python dict data to DynamoDB json dict.
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
