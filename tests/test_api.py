# -*- coding: utf-8 -*-

from fast_dynamodb_json import api


def test():
    _ = api
    _ = api.DATA_TYPE
    _ = api.Integer
    _ = api.Float
    _ = api.String
    _ = api.Binary
    _ = api.Bool
    _ = api.Null
    _ = api.Set
    _ = api.List
    _ = api.Struct
    _ = api.T_ITEM
    _ = api.T_JSON
    _ = api.T_SIMPLE_SCHEMA
    _ = api.T_POLARS_SCHEMA


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_cov_test

    run_cov_test(__file__, "fast_dynamodb_json.api", preview=False)
