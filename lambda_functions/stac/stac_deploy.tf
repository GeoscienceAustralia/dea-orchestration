provider "aws" {
  region = "ap-southeast-2"
  profile = "devProfile"
}

resource "aws_sqs_queue" "stac_queue" {
  name = "static-stac-queue"
}

resource "aws_sqs_queue_policy" "stac_queue_policy" {
  queue_url = "${aws_sqs_queue.stac_queue.id}"
  policy = <<POLICY
{
  "Version": "2018-11-20",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:*:*:static-stac-queue",
      "Condition": {
        "ArnEquals": { "aws:SourceArn": "${aws_s3_bucket.dea_public_data_dev.arn}" }
      }
    }
  ]
}
POLICY
}

resource "aws_s3_bucket" "dea_public_data_dev" {
  # to be filled by importing the existing bucket
}

resource "aws_s3_bucket_notification" "yaml_notification" {
  bucket = "${aws_s3_bucket.dea_public_data_dev.id}"

  queue {
    queue_arn     = "${aws_sqs_queue.stac_queue.arn}"
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".yaml"
  }
}

