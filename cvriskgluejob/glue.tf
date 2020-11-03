
locals{    
    RISK_SCRIPT_LOCATION= "s3://${var.s3_bucket_name}/risk/glue/gluescript.py"
}  

resource "aws_glue_job" "etl" {
  name     = var.risk_job_name
  role_arn = aws_iam_role.cvrisk_glue_role.arn

  command {
    script_location = local.RISK_SCRIPT_LOCATION
  }

  default_arguments = {
    "--job-language"    = "python"
    "--risktable"       = var.DM_RISK_TABLE_NAME
    "--convertBucket "  = "True"
    "--bucket"          = var.s3_bucket_name
  }
}

resource "aws_dynamodb_table" "cvriskresult" {
  name             = var.DM_RISK_TABLE_NAME
  hash_key         = "s3url"
  billing_mode     = "PAY_PER_REQUEST"
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "s3url"
    type = "S"
  }
}

resource "aws_dynamodb_table" "cvriskqueue" {
  name             = var.DM_RISK_QUEUE_TABLE_NAME
  hash_key         = "id"
  billing_mode     = "PAY_PER_REQUEST"
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "id"
    type = "S"
  }
}