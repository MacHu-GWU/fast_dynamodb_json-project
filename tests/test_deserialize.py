# -*- coding: utf-8 -*-

from fast_dynamodb_json.tests.case import CaseEnum


def _test():
    # CaseEnum.case1.test_deserialize()
    # CaseEnum.case2.test_deserialize()
    # CaseEnum.case3.test_deserialize()
    # CaseEnum.case4.test_deserialize()
    # CaseEnum.case5.test_deserialize()
    # CaseEnum.case6.test_deserialize()
    # CaseEnum.case7.test_deserialize()
    # CaseEnum.case8.test_deserialize()
    # CaseEnum.case9.test_deserialize()
    # CaseEnum.case10.test_deserialize()
    # CaseEnum.case11.test_deserialize()
    CaseEnum.case12.test_deserialize()


def _test_all():
    for name, case in CaseEnum.items():
        # print(f"\n==================== {name} ====================")
        case.test_deserialize()


def test():
    print("")
    _test()
    _test_all()


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_cov_test

    run_cov_test(__file__, "fast_dynamodb_json.deserialize", preview=False)
