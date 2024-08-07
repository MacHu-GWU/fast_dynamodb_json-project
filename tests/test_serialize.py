# -*- coding: utf-8 -*-

from fast_dynamodb_json.tests.case import CaseEnum


def test():
    # CaseEnum.case101.test_serialize()
    # CaseEnum.case102.test_serialize()
    # CaseEnum.case103.test_serialize()
    # CaseEnum.case104.test_serialize()
    # CaseEnum.case105.test_serialize()
    # CaseEnum.case106.test_serialize()
    # CaseEnum.case107.test_serialize()
    # CaseEnum.case108.test_serialize()
    CaseEnum.case109.test_serialize()


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_cov_test

    run_cov_test(__file__, "fast_dynamodb_json.serialize", preview=False)
