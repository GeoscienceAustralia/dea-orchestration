provider "aws" {
  region = "ap-southeast-2"
  profile = "devProfile"
}

resource "aws_sqs_queue" "stac_queue" {
  name = "static-stac-queue"
  visibility_timeout_seconds = 600
}

resource "aws_sqs_queue_policy" "stac_queue_policy" {
  queue_url = "${aws_sqs_queue.stac_queue.id}"
  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:*:*:static-stac-queue",
      "Condition": {
        "ArnEquals": { "aws:SourceArn": "${data.aws_s3_bucket.dea_s3_bucket.arn}" }
      }
    }
  ]
}
POLICY
}

data "aws_s3_bucket" "dea_s3_bucket" {
  bucket = "dea-public-data-dev"
}

resource "aws_s3_bucket_notification" "yaml_notification" {
  bucket = "${data.aws_s3_bucket.dea_s3_bucket.id}"

  queue {
    queue_arn     = "${aws_sqs_queue.stac_queue.arn}"
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".yaml"
  }
}

