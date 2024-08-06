# -*- coding: utf-8 -*-

import polars as pl
import fast_dynamodb_json.schema as types

from fast_dynamodb_json.tests.case import CaseEnum


def test():
    CaseEnum.case1.test_dynamodb_json()
    CaseEnum.case1.test_deserialize()


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_unit_test

    run_unit_test(__file__)
