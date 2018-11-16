#!/usr/bin/env bash

serverless invoke local --function stac --path test_data.json
#python notify_to_stac_queue.py -b dea-public-data-dev \
#fractional-cover/fc/v2.2.0/ls8/x_-12/y_-12/2018/02/22/LS8_OLI_FC_3577_-12_-12_20180222125938.yaml