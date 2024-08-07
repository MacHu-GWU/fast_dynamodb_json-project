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
    node: T.Optional["pl.Expr"] = pl.col("Item"),
    is_set: bool = False,
    is_list: bool = False,
) -> T.Optional["pl.Expr"]:
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
            expr = get_selector(name=key, dtype=vtype, node=new_node)
            fields.append(expr)
        final_expr = pl.struct(*fields)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr
    else:
        return None


def deserialize_df(
    df: pl.DataFrame,
    simple_schema: T_SIMPLE_SCHEMA,
    dynamodb_json_col: str = "Item",
) -> pl.DataFrame:
    selectors = []
    names = []
    for name, t in simple_schema.items():
        selector = get_selector(
            name, t, node=pl.col(dynamodb_json_col).struct.field(name)
        )
        if selector is not None:
            selectors.append(selector)
            names.append(name)

    for name, selector in zip(names, selectors):
        print(f"--- expr of field({name!r}) ---")
        print(selector)

    return df.with_columns(*selectors).drop(dynamodb_json_col)


def deserialize(
    records: T.Iterable[T_ITEM],
    simple_schema: T_SIMPLE_SCHEMA,
) -> T.List[T_JSON]:
    """
    Convert regular Python dict data to DynamoDB json dict.
    """
    tmp_col = "Json"
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
