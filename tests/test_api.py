# -*- coding: utf-8 -*-

from fast_dynamodb_json import api


def test():
    _ = api


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_cov_test

    run_cov_test(__file__, "fast_dynamodb_json.api", preview=False)
