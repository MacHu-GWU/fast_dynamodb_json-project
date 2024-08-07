# -*- coding: utf-8 -*-

import typing as T
import polars as pl

if T.TYPE_CHECKING:  # pragma: no cover
    from .schema import DATA_TYPE


T_ITEM = T.Dict[str, T.Any]
T_JSON = T.Dict[str, T.Optional[T.Dict[str, T.Any]]]
T_SIMPLE_SCHEMA = T.Dict[str, "DATA_TYPE"]
T_POLARS_SCHEMA = T.Dict[str, pl.DataType]
