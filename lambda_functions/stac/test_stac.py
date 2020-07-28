"""
This Pytest  script tests stac_parent_update.py, notify_to_stac_queue.py as well as
the serverless lambda function given in stac.py
"""
import json

import boto3
import pytest
from moto import mock_s3, mock_sqs
from pathlib import Path

from stac_parent_update import StacCollections


# When using PyTest fixtures defined in the same file, they must redefine their name.
# pylint: disable=redefined-outer-name


@pytest.fixture
def s3_dataset_yamls():
    """
    Return a list of dict of test dataset info from various products
    """

    return [
        {
            "name": "fractional-cover/fc/v2.2.0/ls8/x_-12/y_-12/2018/02/22/"
            "LS8_OLI_FC_3577_-12_-12_20180222015938000000_v1521547282.yaml",
            "prefixes": [
                "fractional-cover/fc/v2.2.0/ls8/x_-12",
                "fractional-cover/fc/v2.2.0/ls8/x_-12/y_-12",
            ],
        },
        {
            "name": "fractional-cover/fc/v2.2.0/ls5/x_-10/y_-12/1998/10/22/"
            "LS5_TM_FC_3577_-10_-12_1998_v20171127041658_47.yaml",
            "prefixes": [
                "fractional-cover/fc/v2.2.0/ls5/x_-10",
                "fractional-cover/fc/v2.2.0/ls5/x_-10/y_-12",
            ],
        },
        {
            "name": "fractional-cover/fc-percentile/annual/v2.1.0/combined/x_-12/y_-24/2010/"
            "LS_FC_PC_3577_-12_-24_20100101.yaml",
            "prefixes": [
                "fractional-cover/fc-percentile/annual/v2.1.0/combined/x_-12",
                "fractional-cover/fc-percentile/annual/v2.1.0/combined/x_-12/y_-24",
            ],
        },
        {
            "name": "fractional-cover/fc-percentile/seasonal/v2.1.0/combined/x_-14/y_-22/201712/"
            "LS_FC_PC_3577_-14_-22_20171201_20180228.yaml",
            "prefixes": [
                "fractional-cover/fc-percentile/seasonal/v2.1.0/combined/x_-14",
                "fractional-cover/fc-percentile/seasonal/v2.1.0/combined/x_-14/y_-22",
            ],
        },
        {
            "name": "WOfS/WOFLs/v2.1.5/combined/x_-12/y_-15/2013/12/02/"
            "LS_WATER_3577_-12_-15_20131202015607000000_v1526758996.yaml",
            "prefixes": [
                "WOfS/WOFLs/v2.1.5/combined/x_-12",
                "WOfS/WOFLs/v2.1.5/combined/x_-12/y_-15",
            ],
        },
        {
            "name": "WOfS/annual_summary/v2.1.5/combined/x_-10/y_-12/2012/WOFS_3577_-10_-12_2012_summary.yaml",
            "prefixes": [
                "WOfS/annual_summary/v2.1.5/combined/x_-10",
                "WOfS/annual_summary/v2.1.5/combined/x_-10/y_-12",
            ],
        },
        {
            "name": "WOfS/filtered_summary/v2.1.0/combined/x_-10/y_-12/wofs_filtered_summary_-10_-12.yaml",
            "prefixes": [
                "WOfS/filtered_summary/v2.1.0/combined/x_-10",
                "WOfS/filtered_summary/v2.1.0/combined/x_-10/y_-12",
            ],
        },
        {
            "name": "WOfS/summary/v2.1.0/combined/x_-18/y_-22/WOFS_3577_-18_-22_summary.yaml",
            "prefixes": [
                "WOfS/summary/v2.1.0/combined/x_-18",
                "WOfS/summary/v2.1.0/combined/x_-18/y_-22",
            ],
        },
        {
            "name": "item_v2/v2.0.1/relative/lon_114/lat_-22/ITEM_REL_271_114.36_-22.31.yaml",
            "prefixes": [
                "item_v2/v2.0.1/relative/lon_114",
                "item_v2/v2.0.1/relative/lon_114/lat_-22",
            ],
        },
        {
            "name": "mangrove_cover/-11_-20/MANGROVE_COVER_3577_-11_-20_20170101.yaml",
            "prefixes": ["mangrove_cover/-11_-20"],
        },
    ]


@mock_s3
@mock_sqs
def test_s3_event_handler():
    bucket_name = "dea-public-data-dev"
    key = "test-prefix/dir/x_-5/y_-23/2010/02/13/LS5_TM_FC_3577_-5_-23_20100213122216.yaml"
    s3 = boto3.resource("s3")
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    bucket = s3.create_bucket(Bucket=bucket_name, ACL="public-read")

    bucket.upload_file(
        str(Path(__file__).parent / "tests/LS5_TM_FC_3577_-5_-23_20100213012240.yaml"),
        key,
    )

    event_body = {
        "Records": [{"s3": {"bucket": {"name": bucket_name}, "object": {"key": key}}}]
    }
    event = {"Records": [{"body": json.dumps(event_body)}]}

    import stac

    stac.CFG = TEST_CONFIG
    stac.stac_handler(event, context={})

    expected_key = key.replace(".yaml", "_STAC.json")
    obj = bucket.Object(expected_key)

    assert obj.content_type == "application/json"
    stac_json = json.load(obj.get()["Body"])
    assert "id" in stac_json
    assert "geometry" in stac_json
    assert stac_json["type"] == "Feature"
    assert stac_json["id"] == "b820133f-387e-48cb-9425-1ae038123911"
    assert len(stac_json["assets"]) == 4
    assert {"BS", "NPV", "PV", "UE"} == set(stac_json["assets"])

    links = {link["rel"]: link["href"] for link in stac_json["links"]}
    assert "self" in links
    assert "parent" in links
    assert links["self"].endswith("STAC.json")
    assert links["parent"].endswith("catalog.json")


TEST_CONFIG = {
    "products": [
        {
            "name": "test-product",
            "prefix": "test-prefix/dir",
            "description": "must have description",
            "catalog_structure": ["x_{x}", "x_{x}/y_{y}"],
        }
    ],
    "license": {
        "short_name": "friendly license",
        "name": "License name",
        "copyright": "Copyright",
    },
    "aws-domain": "https://sub.example.com",
    "root-catalog": "https://sub.example.com/catalog.json",
    "aus-extent": {"spatial": [108, -45, 155, -10], "temporal": [None, None]},
    "contact": {"name": "Mrs Test Contact"},
    "homepage": "https://example.com/",
}


@mock_s3
def test_creating_catalogs():
    bucket_name = "dea-public-data-dev"
    s3 = boto3.resource("s3")
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    bucket = s3.create_bucket(Bucket=bucket_name)

    keys = [
        "test-prefix/dir/x_-5/y_-23/2010/02/13/foo1.yaml",
        "test-prefix/dir/x_-5/y_-23/2010/02/13/foo2.yaml",
        "test-prefix/dir/x_4/y_2/2010/02/13/foo3.yaml",
    ]

    cu = StacCollections(TEST_CONFIG)
    cu.add_items(keys)
    cu.persist_all_catalogs(bucket_name)

    objects = list(bucket.objects.all())
    for o in objects:
        print(o)
        body = json.load(o.get()["Body"])
        print(json.dumps(body, indent=4))

    assert len(objects) == 5

    # Check Top Level Catalog
    collection = json.load(
        bucket.Object(key="test-prefix/dir/catalog.json").get()["Body"]
    )
    assert len(collection["links"]) == 5

    child_links = [link for link in collection["links"] if link["rel"] == "child"]
    assert len(child_links) == 2

    assert all(link["href"].endswith("catalog.json") for link in collection["links"])
    assert "license" in collection

    # Check common properties of all catalogs
    for o in objects:
        body = json.load(o.get()["Body"])
        assert body["stac_version"] == "0.6.0"
        assert "id" in body
        assert "description" in body
        assert "links" in body
        assert body["description"] == "must have description"

        assert all("href" in link and "rel" in link for link in body["links"])

        assert any(link["rel"] == "self" for link in body["links"])
