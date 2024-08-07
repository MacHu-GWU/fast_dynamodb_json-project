# -*- coding: utf-8 -*-

from fast_dynamodb_json.tests.case import CaseEnum


def _test():
    # CaseEnum.case1.test_serialize()
    # CaseEnum.case101.test_serialize()
    # CaseEnum.case102.test_serialize()
    # CaseEnum.case103.test_serialize()
    # CaseEnum.case104.test_serialize()
    CaseEnum.case105.test_serialize()


def _test_all():
    for name, case in CaseEnum.items():
        # print(f"\n==================== {name} ====================")
        case.test_serialize()


def test():
    print("")
    _test()
    # _test_all()


if __name__ == "__main__":
    from fast_dynamodb_json.tests import run_cov_test

    run_cov_test(__file__, "fast_dynamodb_json.deserialize", preview=False)
