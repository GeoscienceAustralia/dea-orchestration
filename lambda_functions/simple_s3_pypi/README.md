# Simple S3 Triggers

This directory contains code for 2 AWS Lambda functions:
<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Generate Listing](#generate-listing)
- [Make Public](#make-public)
- [Installation Instructions](#installation-instructions)
- [Problems](#problems)

<!-- markdown-toc end -->

## Generate Listing

This Lambda was to maintain a simple python package server hosted on S3.

Rather than attempting to fix those problems. This takes a new approach,
similar to [s3-directory-listing](https://github.com/razorjack/s3-directory-listing),
which uses a Lambda function triggered on any uploads to a particular S3 bucket.

It should be as simple as uploading a couple of package files. Upload files
either manually, or from CI tools, and the directory listings will be
automatically updated.

We can follow [PEP 503 - Simple Repository API](https://www.python.org/dev/peps/pep-0503/)
and maybe eventually include extra niceties like checksum hashes.

Run this tool from the command line to manually update `index.html` files.

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
