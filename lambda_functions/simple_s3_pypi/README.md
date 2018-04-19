This Lambda is responsible for maintaining a simple python package
server hosted on S3.

Originally we attempted to use s3pypi to build and upload packages,
and also maintain the essential `index.html` pages.

However, it causes a few issues. It doesn't play nicely with Fractional Cover
which needs to build a binary package from some Fortran sources. And more importantly
it has a very strict and hard coded restriction on package version numbers,
which causes most of our packages to go unlisted.

Rather than attempting to fix those problems. This takes a new approach,
similar to [s3-directory-listing](https://github.com/razorjack/s3-directory-listing),
which uses a Lambda function triggered on any uploads to a particular S3 bucket.

It should be as simple as uploading a couple of package files, which can
be done manually from the CLI, from a GUI, or with build in Travis-CI
deployment tools.

We can follow [PEP 503 - Simple Repository API](https://www.python.org/dev/peps/pep-0503/)
and maybe eventually include extra niceties like checksum hashes.