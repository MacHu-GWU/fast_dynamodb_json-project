
.. image:: https://readthedocs.org/projects/fast-dynamodb-json/badge/?version=latest
    :target: https://fast-dynamodb-json.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/MacHu-GWU/fast_dynamodb_json-project/actions/workflows/main.yml/badge.svg
    :target: https://github.com/MacHu-GWU/fast_dynamodb_json-project/actions?query=workflow:CI

.. image:: https://codecov.io/gh/MacHu-GWU/fast_dynamodb_json-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/fast_dynamodb_json-project

.. image:: https://img.shields.io/pypi/v/fast-dynamodb-json.svg
    :target: https://pypi.python.org/pypi/fast-dynamodb-json

.. image:: https://img.shields.io/pypi/l/fast-dynamodb-json.svg
    :target: https://pypi.python.org/pypi/fast-dynamodb-json

.. image:: https://img.shields.io/pypi/pyversions/fast-dynamodb-json.svg
    :target: https://pypi.python.org/pypi/fast-dynamodb-json

.. image:: https://img.shields.io/badge/Release_History!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/fast_dynamodb_json-project/blob/main/release-history.rst

.. image:: https://img.shields.io/badge/STAR_Me_on_GitHub!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/fast_dynamodb_json-project

------

.. image:: https://img.shields.io/badge/Link-Document-blue.svg
    :target: https://fast-dynamodb-json.readthedocs.io/en/latest/

.. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://fast-dynamodb-json.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/fast_dynamodb_json-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/fast_dynamodb_json-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/fast_dynamodb_json-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/fast-dynamodb-json#files


Welcome to ``fast_dynamodb_json`` Documentation
==============================================================================
.. image:: https://fast-dynamodb-json.readthedocs.io/en/latest/_static/fast_dynamodb_json-logo.png
    :target: https://fast-dynamodb-json.readthedocs.io/en/latest/

AWS DynamoDB is a powerful NoSQL database service, but it comes with a unique challenge: its data is not stored in standard JSON format. Instead, DynamoDB encodes everything as strings. For example, a string "hello" is encoded as `{"S": "hello"}`, and a number 123 as `{"N": "123"}`. This encoding necessitates deserialization into regular Python dictionaries for data processing, which can be a significant bottleneck in high-volume operations.

Existing Python libraries like `pynamodb` and `dynamodb_json` offer solutions, but their implementations rely on Python for-loops and recursive programming. While functional, these approaches struggle with performance when handling large volumes of DynamoDB items. Moreover, their design doesn't lend itself well to vectorized computing or optimization through JIT technologies like PyPy or Numba.

As the founder of a startup and a former Amazon Web Services data architect, I've encountered this challenge firsthand. Our company operates an analytics data lake and data pipeline processing billions of DynamoDB items daily for our customers. This scale demanded a more efficient solution.

Enter `fast_dynamodb_json` - a revolutionary approach to deserializing large quantities of DynamoDB items. Our benchmarks demonstrate that this new method is up to 5000 times faster than pure Python implementations, maintaining this impressive speed advantage across both simple and complex datasets.

This project aims to dramatically improve the efficiency of DynamoDB data processing, enabling businesses to handle massive datasets with unprecedented speed and resource efficiency.


.. _install:

Install
------------------------------------------------------------------------------

``fast_dynamodb_json`` is released on PyPI, so all you need is to:

.. code-block:: console

    $ pip install fast-dynamodb-json

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade fast-dynamodb-json
