# -*- coding: utf-8 -*-

"""
This script is to compare the performance of parsing dynamodb export data
from s3 using dynamodb_json and polars.

Dataset:

- 1M records of amazon order data across one year in total.
- 7x compressed ndjson file, 143K records each.
- each ``.json.gz`` file is around 19.3MB, 135MB uncompressed.
- total file is 135MB compressed, 945MB uncompressed.
- if we dump it to parquet, it will be 17MB each (143K records).
- Each file takes around 2.5s to process (using the fastest method)

Sample Record::

    {
        "CustomerID": "CUST-006076",
        "OrderID": "ORD-000116602",
        "OrderDate": "2023-01-23T01:49:21",
        "EstimatedDeliveryDate": "2023-02-06T01:49:21",
        "Status": "Shipped",
        "Items": [
            {
                "Name": "He house",
                "Price": 194.99,
                "ProductID": "PROD-000285",
                "Quantity": 3
            }
        ],
        "PaymentMethod": "PayPal",
        "LastFourDigits": "8310",
        "AppliedCoupons": [],
        "ShippingAddress": {
            "City": "West Nicolas",
            "Country": "USA",
            "State": "MH",
            "StreetAddress": "812 Christy Way Apt. 764",
            "ZipCode": "15728"
        },
        "TotalAmount": 264.29,
        "GiftMessage": "Style indeed operation wait fine up.",
        "GiftWrap": true
    }

Conclusion:

1. read directly from s3 is faster than download to local and read from local.
2. polars is 5 ~ 10x faster than dynamodb_json, and will be even faster when
    the data structure is complex.

Estimation on Billion Records:

- 4.3B records (from 2018 Amazon sales statistics).
- each ``.json.gz`` file is around 19.3MB, 135MB uncompressed.
- will generate around 30100 ndjson file like this.
- total file is 580GB compressed, 4TB uncompressed.
- all data in datalake in parquet format will be around 512GB.
- if we split 1 year data into 365 partition, then each partition is around 1.4GB.
- Using 100 concurrent workers on AWS Lambda, total processing time is 13 minutes (For 512GB datalake).
- Total Lambda Cost $1.28.

See details at: https://docs.google.com/spreadsheets/d/1BIea3iwm_wkaVxQW4UUv6vbp3qpm84WYUwPhlgZ0BZ4/edit?gid=0#gid=0
"""

import json
import gzip
import polars as pl
from pathlib import Path
from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager
from dynamodb_json import json_util
from rich import print as rprint

from fast_dynamodb_json.api import (
    Integer,
    Float,
    String,
    Binary,
    Bool,
    Null,
    Set,
    List,
    Struct,
    deserialize_df,
)
from fast_dynamodb_json.vendor.polars_utils import pprint_df
from fast_dynamodb_json.vendor.timer import DateTimeTimer


aws_profile = "bmt_app_dev_us_east_1"
dynamodb_export_s3uri = (
    "s3://bmt-app-dev-us-east-1-data"
    "/projects/aws_s3_dynamodb_data_analysis"
    "/examples/ecommerce_orders/exports"
    "/AWSDynamoDB/01722632920672-dd1ec76a/data/"
)
bsm = BotoSesManager(profile_name=aws_profile)
context.attach_boto_session(bsm.boto_ses)

simple_schema = {
    "OrderID": String(),
    "CustomerID": String(),
    "OrderDate": String(),
    "TotalAmount": Float(),
    "Status": String(),
    "ShippingAddress": Struct(
        {
            "StreetAddress": String(),
            "City": String(),
            "State": String(),
            "ZipCode": String(),
            "Country": String(),
        }
    ),
    "Items": List(
        Struct(
            {
                "ProductID": String(),
                "Name": String(),
                "Price": Float(),
                "Quantity": Integer(),
            }
        ),
    ),
    "AppliedCoupons": List(String()),
    "PaymentMethod": String(),
    "LastFourDigits": String(),
    "EstimatedDeliveryDate": String(),
    "GiftWrap": Bool(),
    "GiftMessage": String(),
}


s3dir = S3Path(dynamodb_export_s3uri)
s3path_list = s3dir.iter_objects().all()
dir_tmp = Path(__file__).absolute().parent / "tmp"
dir_tmp.mkdir(exist_ok=True)

pl_schema = {
    "Item": pl.Struct(
        {name: dtype.to_dynamodb_json_polars() for name, dtype in simple_schema.items()}
    ),
}

for s3path in s3path_list:
    path = dir_tmp / s3path.basename
    bsm.s3_client.download_file(s3path.bucket, s3path.key, str(path))

# ------------------------------------------------------------------------------
# Work on one file 145k records.
#
# Measurement:
#
# {
#   'dynamodb_json_use_pathlib': 16.938016,
#   'dynamodb_json_use_polars': 21.076709,
#   'polars': 2.377557
# }
# ------------------------------------------------------------------------------
# use pathlib + json to read ndjson
with DateTimeTimer("use dynamodb_json method 1 ...") as timer:
    records = list()
    with open(path, "rb") as f:
        lines = gzip.decompress(f.read()).splitlines()
        for line in lines:
            record = json_util.loads(json.loads(line)["Item"])
            records.append(record)
            # break
elapse1 = timer.elapsed
rprint(json.dumps(records[0], indent=4, sort_keys=True))

# use polars to read ndjson
with DateTimeTimer("use dynamodb_json method 2 ...") as timer:
    records = list()
    df = pl.read_ndjson(str(path), schema=pl_schema)
    for row in df.to_dicts():
        record = json_util.loads(row["Item"])
        records.append(record)
        # break
elapse2 = timer.elapsed
rprint(json.dumps(records[0], indent=4, sort_keys=True))

with DateTimeTimer("use polars ...") as timer:
    df = pl.read_ndjson(str(path), schema=pl_schema)
    df = deserialize_df(df, simple_schema)
    records = df.to_dicts()
elapse3 = timer.elapsed
rprint(json.dumps(records[0], indent=4, sort_keys=True))

metrics = {
    "dynamodb_json_use_pathlib": elapse1,
    "dynamodb_json_use_polars": elapse2,
    "polars": elapse3,
}
print(metrics)


# ------------------------------------------------------------------------------
# Work on 7 file 1M records.
#
# Measurement
#
# {
#   'dynamodb_json_use_pathlib': 120.469504,
#   'polars': 25.192146
# }
# ------------------------------------------------------------------------------
with DateTimeTimer("use dynamodb_json ...") as timer:
    records = list()
    for path in dir_tmp.iterdir():
        with open(path, "rb") as f:
            lines = gzip.decompress(f.read()).splitlines()
            for line in lines:
                record = json_util.loads(json.loads(line)["Item"])
                records.append(record)
                # break
        # break
elapse1 = timer.elapsed
rprint(json.dumps(records[0], indent=4, sort_keys=True))
print(len(records))

with DateTimeTimer("use polars ...") as timer:
    df = pl.scan_ndjson(
        f"{dir_tmp}/*.json.gz",
        schema=pl_schema,
    ).collect()
    df = deserialize_df(df, simple_schema)
    records = df.to_dicts()
elapse3 = timer.elapsed
rprint(json.dumps(records[0], indent=4, sort_keys=True))
print(len(records))

metrics = {
    # "dynamodb_json_use_pathlib": elapse1,
    "polars": elapse3,
}
print(metrics)
