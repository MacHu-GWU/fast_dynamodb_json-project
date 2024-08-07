# -*- coding: utf-8 -*-

import polars as pl
from tabulate import tabulate


def df_to_ascii(df: pl.DataFrame) -> str:
    return tabulate(
        [
            list(record.values())
            for record in df.to_dicts()
        ],
        headers=list(df.schema),
        tablefmt="grid",
    )


def pprint_df(df: pl.DataFrame):
    print(df_to_ascii(df))
