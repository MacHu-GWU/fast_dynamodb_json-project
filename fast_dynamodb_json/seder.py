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


def to_expr_1(
    name: T.Optional[str],
    t: pl.DataType,
    start: T.Optional["pl.Expr"] = pl.col("Item"),
    is_set: bool = False,
    is_list: bool = False,
) -> pl.Expr:
    # fmt: off
    if isinstance(t, pl.Utf8):
        if is_set:
            return pl.element()
        elif is_list:
            return start.struct.field("S")
        else:
            return start.struct.field("S").alias(name)
    elif isinstance(t, pl.Int64):
        if is_set:
            return pl.element().cast(pl.Int64)
        elif is_list:
            return start.struct.field("N").cast(pl.Int64)
        else:
            return start.struct.field("N").cast(pl.Int64).alias(name)
    elif isinstance(t, pl.Float64):
        if is_set:
            return pl.element().cast(pl.Float64)
        elif is_list:
            return start.struct.field("N").cast(pl.Float64)
        else:
            return start.struct.field("N").cast(pl.Float64).alias(name)
    elif isinstance(t, pl.Binary):
        if is_set:
            return pl.element().cast(pl.Binary).bin.decode("base64")
        elif is_list:
            return start.struct.field("B").cast(pl.Binary).bin.decode("base64")
        else:
            return start.struct.field("B").cast(pl.Binary).bin.decode("base64").alias(name)
    elif isinstance(t, pl.Boolean):
        return start.struct.field("BOOL").alias(name)
    elif isinstance(t, pl.Null):
        return pl.lit(None).alias(name)

    # --------------------------------------------------------------------------
    # Set
    # --------------------------------------------------------------------------
    elif isinstance(t, pl.Array):
        if isinstance(t.inner, pl.Utf8):
            field = "SS"
        elif isinstance(t.inner, pl.Int64):
            field = "NS"
        elif isinstance(t.inner, pl.Float64):
            field = "NS"
        elif isinstance(t.inner, pl.Binary):
            field = "BS"
        else:
            raise NotImplementedError
        expr = to_expr(name=None, t=t.inner, start=None, is_set=True)
        final_expr = start.struct.field(field).list.eval(expr)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr

    # --------------------------------------------------------------------------
    # List
    # --------------------------------------------------------------------------
    elif isinstance(t, pl.List):
        expr = to_expr(name=None, t=t.inner, start=pl.element(), is_list=True)
        final_expr = start.struct.field("L").list.eval(expr)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr

    # --------------------------------------------------------------------------
    # Struct
    # --------------------------------------------------------------------------
    elif isinstance(t, pl.Struct):
        fields = list()
        for field in t.fields:
            new_start = start.struct.field("M").struct.field(field.name)
            expr = to_expr(name=field.name, t=field.dtype, start=new_start)
            fields.append(expr)
        final_expr = pl.struct(*fields)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr


def to_expr(
    name: T.Optional[str],
    t: DATA_TYPE,
    start: T.Optional["pl.Expr"] = pl.col("Item"),
    is_set: bool = False,
    is_list: bool = False,
) -> pl.Expr:
    # fmt: off
    if isinstance(t, String):
        if is_set:
            return pl.element()
        elif is_list:
            return start.struct.field("S")
        else:
            return start.struct.field("S").alias(name)
    elif isinstance(t, Integer):
        if is_set:
            return pl.element().cast(pl.Int64)
        elif is_list:
            return start.struct.field("N").cast(pl.Int64)
        else:
            return start.struct.field("N").cast(pl.Int64).alias(name)
    elif isinstance(t, Float):
        if is_set:
            return pl.element().cast(pl.Float64)
        elif is_list:
            return start.struct.field("N").cast(pl.Float64)
        else:
            return start.struct.field("N").cast(pl.Float64).alias(name)
    elif isinstance(t, Binary):
        if is_set:
            return pl.element().cast(pl.Binary).bin.decode("base64")
        elif is_list:
            return start.struct.field("B").cast(pl.Binary).bin.decode("base64")
        else:
            return start.struct.field("B").cast(pl.Binary).bin.decode("base64").alias(name)
    elif isinstance(t, Bool):
        return start.struct.field("BOOL").alias(name)
    elif isinstance(t, Null):
        return pl.lit(None).alias(name)

    # --------------------------------------------------------------------------
    # Set
    # --------------------------------------------------------------------------
    elif isinstance(t, Set):
        if isinstance(t.itype, String):
            field = "SS"
        elif isinstance(t.itype, Integer):
            field = "NS"
        elif isinstance(t.itype, Float):
            field = "NS"
        elif isinstance(t.itype, Binary):
            field = "BS"
        else:
            raise NotImplementedError
        expr = to_expr(name=None, t=t.itype, start=None, is_set=True)
        final_expr = start.struct.field(field).list.eval(expr)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr

    # --------------------------------------------------------------------------
    # List
    # --------------------------------------------------------------------------
    elif isinstance(t, List):
        expr = to_expr(name=None, t=t.itype, start=pl.element(), is_list=True)
        final_expr = start.struct.field("L").list.eval(expr)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr

    # --------------------------------------------------------------------------
    # Struct
    # --------------------------------------------------------------------------
    elif isinstance(t, Struct):
        fields = list()
        # for field in t.types:
        for key, vtype in t.types.items():
            new_start = start.struct.field("M").struct.field(key)
            expr = to_expr(name=key, t=vtype, start=new_start)
            fields.append(expr)
        final_expr = pl.struct(*fields)
        if name:
            final_expr = final_expr.alias(name)
        return final_expr


def deserialize_df(
    df: pl.DataFrame,
    simple_schema: T_SIMPLE_SCHEMA,
    dynamodb_json_col: str = "Item",
) -> pl.DataFrame:
    exprs = []
    for name, t in simple_schema.items():
        expr = to_expr(name, t, start=pl.col(dynamodb_json_col).struct.field(name))
        if expr is not None:
            exprs.append(expr)
    return df.with_columns(*exprs).drop(dynamodb_json_col)


def deserialize(
    records: T.Iterable[T_ITEM],
    simple_schema: T_SIMPLE_SCHEMA,
) -> T.List[T_JSON]:
    """
    Convert regular Python dict data to DynamoDB json dict.
    """
    tmp_col = "Json"
    df = pl.DataFrame([{tmp_col: record} for record in records], strict=False)
    df = deserialize_df(df=df, simple_schema=simple_schema, dynamodb_json_col=tmp_col)
    return df.to_dicts()


# def deserialize(
#     df: pl.DataFrame,
#     schema: T.Dict[str, T.Any],
#     item_col: str = "Item",
# ) -> pl.DataFrame:
#     exprs = []
#     for name, t in schema.items():
#         expr = to_expr(name, t, start=pl.col(item_col).struct.field(name))
#         if expr is not None:
#             exprs.append(expr)
#     # --- for debug only
#     # print("=" * 80)
#     # for ith, expr in enumerate(exprs, start=1):
#     #     print(f"--- {ith} ---")
#     #     print(expr)
#     return df.with_columns(*exprs).drop(item_col)
