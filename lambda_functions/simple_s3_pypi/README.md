# Simple S3 Triggers

This directory contains code for 2 AWS Lambda functions:

The first, Generate Listing, automatically generates `index.html` objects to
make an AWS S3 bucket easily browsed. We use it for running a simple Python
package repository.

The second, Make Public, changes the permissions for any objects created in an
S3 Bucket. We use it to keep our S3 Inventory objects public.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc-refresh-toc -->
**Table of Contents**

- [Generate Listing](#generate-listing)
- [Make Public](#make-public)
- [Installation Instructions](#installation-instructions)
- [Problems](#problems)

<!-- markdown-toc end -->

## Installation Instructions

Install `npm` and `serverless`.

From a shell configured for accessing the production AWS account, run:


```shell
npm install
serverless deploy
serverless s3deploy
```
## Generate Listing

This Lambda maintains a simple python package repository, hosted on S3.

It works by keeping `index.html` files updated in a bucket, as new objects appear.
It draws inspiration from
[s3-directory-listing](https://github.com/razorjack/s3-directory-listing), which
does the same thing but is written in Javascript.

Added new packages or releases is as simple as uploading files to the bucket.
either manually, or from CI tools, and the directory listings will be
automatically updated.

Run this tool from the command line to manually update `index.html` files.

To update the root of our package repository

    python genindex.py datacube-core-deployment

## Make Public

This Lambda is configured to make any object placed into `s3://dea-public-inventory` public.

It's the simplest way to make a public inventory. When AWS S3 Inventory runs,
the objects it creates aren't owned by the bucket owner, which means permissions
set on the bucket aren't applied to the objects.


## Problems

 - We may Need to switch to S3 -> SNS notifications with a fanout, since you
   can't create multiple event triggers from a single S3 Bucket.
 - We can follow [PEP 503 - Simple Repository
API](https://www.python.org/dev/peps/pep-0503/) and maybe eventually include
extra niceties like checksum hashes.
