
locals{
    lambda_zip_location = "output/riskviewlambda.zip"
    DM_RISK_QUEUE_TABLE_NAME = "cvriskqueue"
    DM_RISK_TABLE_NAME = "cvrisk"
    RISK_GLUE_JOB_NAME = "convertcv2risk"
    RISK_TIMER_EVENT_NAME = "scheduled_launch_cvriskjob"
    S3_XFILESBUCKET = "raytestlambda"
}

data "archive_file" "risk_lambda_function" {
  type        = "zip"
  source_file = "lambda_function.py"
  output_path = local.lambda_zip_location
}

resource "aws_lambda_function" "risk_lambda_function" {
  filename      = local.lambda_zip_location
  function_name = "risk_lambda_function"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"

  # The filebase64sha256() function is available in Terraform 0.11.12 and later
  # For Terraform 0.11.11 and earlier, use the base64sha256() function and the file() function:
  # source_code_hash = "${base64sha256(file("lambda_function_payload.zip"))}"
  source_code_hash = filebase64sha256(local.lambda_zip_location)

  runtime = "python3.7"

  environment {
    variables = {
      DM_RISK_QUEUE_TABLE_NAME = local.DM_RISK_QUEUE_TABLE_NAME
      DM_RISK_TABLE_NAME = local.DM_RISK_TABLE_NAME
      RISK_GLUE_JOB_NAME = local.RISK_GLUE_JOB_NAME
      RISK_TIMER_EVENT_NAME = local.RISK_TIMER_EVENT_NAME
      S3_XFILESBUCKET = local.S3_XFILESBUCKET

    }
  }
}