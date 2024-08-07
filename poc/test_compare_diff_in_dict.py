# -*- coding: utf-8 -*-

"""
Demonstrate how to display dictionary difference in a human-readable format.
"""

def test():
    d1 = {
        "id": 1,
        "name": "alice",
    }
    d2 = {
        "id": 1,
        "name": "bob",
    }
    assert d1 == d2


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_unit_test

    run_unit_test(__file__)
