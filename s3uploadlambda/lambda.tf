

data "aws_s3_bucket" "lambda_trigger_bucket" {
  bucket =  var.s3_bucket_name
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = data.aws_s3_bucket.lambda_trigger_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_event_risk_handler.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".csv"
  }
  depends_on = [aws_lambda_permission.allow_bucket]
}

resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_event_risk_handler.arn
  principal     = "s3.amazonaws.com"
  source_arn    = data.aws_s3_bucket.lambda_trigger_bucket.arn
}

data "archive_file" "s3_event_risk_handler" {
  type        = "zip"
  source_file = "lambda_function.py"
  output_path = var.lambda_zip_location
}

resource "aws_lambda_function" "s3_event_risk_handler" {
  filename      = var.lambda_zip_location
  function_name = "s3_event_risk_handler"
  role          = aws_iam_role.lambda_role.arn
  handler       = var.lambda_handler
  description   = "An AWS S3 trigger function that saves cvs in risk queue talbe for the object that has been uploaded."


  # The filebase64sha256() function is available in Terraform 0.11.12 and later
  # For Terraform 0.11.11 and earlier, use the base64sha256() function and the file() function:
  # source_code_hash = "${base64sha256(file("lambda_function_payload.zip"))}"
  source_code_hash = filebase64sha256(var.lambda_zip_location)

  runtime = var.lambda_runtime

  environment {
    variables = {
      DM_RISK_QUEUE_TABLE_NAME = var.DM_RISK_QUEUE_TABLE_NAME
      RISK_TIMER_EVENT_NAME = var.RISK_TIMER_EVENT_NAME

    }
  }
}