# Simple S3 Triggers

This directory contains code for 2 x lambda functions:

1. Generate Listing
2. Make Public

## Generate Listing

This Lambda is responsible for maintaining a simple python package
server hosted on S3.

Rather than attempting to fix those problems. This takes a new approach,
similar to [s3-directory-listing](https://github.com/razorjack/s3-directory-listing),
which uses a Lambda function triggered on any uploads to a particular S3 bucket.

It should be as simple as uploading a couple of package files, which can
be done manually from the CLI, from a GUI, or with build in Travis-CI
deployment tools.

We can follow [PEP 503 - Simple Repository API](https://www.python.org/dev/peps/pep-0503/)
and maybe eventually include extra niceties like checksum hashes.

This tool can be run from the command line to manually update `index.html` files.

To update the root of our package repository

    python genindex.py datacube-core-deployment

## Make Public

Configured to make any object placed into `dea-public-inventory` public.

It's the simplest way to make a public inventory. Objects aren't owned by the bucket owner,
so permissions set on the bucket don't apply to the objects.





## Installation Instructions


```shell
sls deploy && sls s3deploy
```


## Problems

 * _Need to switch to SNS with a fanout, since can't double up on
Events on the same bucket_
