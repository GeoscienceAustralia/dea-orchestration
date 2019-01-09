provider "aws" {
  region = "ap-southeast-2"
  profile = "prodProfile"
}

data "aws_sns_topic" "dea_public_data_topic" {
  name = ""
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
        "ArnEquals": { "aws:SourceArn": "${data.aws_sns_topic.dea_public_data_topic.arn}" }
      }
    }
  ]
}
POLICY
}
