
variable S3_XFILESBUCKET{
    type = string
    default = "raytestlambda"
}
locals{

    risk_job_name = "cvriskgluejob"
    RISK_SCRIPT_LOCATION= "s3://${var.S3_XFILESBUCKET}/risk/glue/gluescript.py"
}  

resource "aws_glue_job" "etl" {
  name     = local.risk_job_name
  role_arn = aws_iam_role.cvrisk_glue_role.arn

  command {
    script_location = local.RISK_SCRIPT_LOCATION
  }

  default_arguments = {
    "--job-language"    = "python"
    "--risktable"       = "cvriskresult"
    "--convertBucket "  = "True"
    "--bucket"          = "raytestlambda"
  }
}

resource "aws_dynamodb_table" "cvriskresult" {
  name             = "cvriskresult"
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
  name             = "cvriskqueue"
  hash_key         = "id"
  billing_mode     = "PAY_PER_REQUEST"
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "id"
    type = "S"
  }
}