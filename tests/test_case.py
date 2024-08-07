# -*- coding: utf-8 -*-

from fast_dynamodb_json.schema import Integer, Float, String

from fast_dynamodb_json.tests.case import Case


def test():
    case = Case(
        item={
            "a_int": 1,
            "a_float": 3.14,
            "a_str": "Alice",
        },
        json={
            "a_int": {"N": "1"},
            "a_float": {"N": "3.14"},
            "a_str": {"S": "Alice"},
        },
        simple_schema={
            "a_int": Integer(),
            "a_float": Float(),
            "a_str": String(),
        },
    )
    case.test_dynamodb_json()
    case.test_deserialize()
    # case.test_serialize()


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_unit_test

    run_unit_test(__file__)
