
provider "aws" {
  region = "ap-southeast-2"
}

variable "function_name" {
  description = "The function name. Is used to work out the name of the zip file to deploy, the name of the lambda to create, and the name of the python module inside the lambda to execute."
}

variable "environment_variables" {
  description = "Environment variables to be passed to the lambda function"
  type = "map"
  default = {
      DEA_MODULE = "dea-prod/20171123"
      PROJECT = "v10"
      QUEUE = "normal"
  }
}

resource "aws_lambda_function" "new_lambda_function" {
  function_name = "${var.function_name}"
  handler = "${var.function_name}.handler"
  runtime = "python3.6"
  filename = "${path.root}/../../dist/${var.function_name}.zip"
  timeout = 60
  source_code_hash = "${base64sha256(file("${path.root}/../../dist/${var.function_name}.zip"))}"
  role = "${aws_iam_role.lambda_exec_role.arn}"

  environment {
    variables = "${var.environment_variables}"
  }
}

resource "aws_iam_role" "lambda_exec_role" {
  name = "orchestration.lambda.${var.function_name}"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "attach-lambda-execution-policy" {
    role       = "${aws_iam_role.lambda_exec_role.name}"
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "attach-pipeline-automation-policy" {
    role       = "${aws_iam_role.lambda_exec_role.name}"
    policy_arn = "arn:aws:iam::538673716275:policy/Pipeline-Automation"
}
