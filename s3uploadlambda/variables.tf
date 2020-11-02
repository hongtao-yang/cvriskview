variable "region" {
  description = "The AWS region we want this bucket to live in."
  default     = "ca-central-1"
}

variable "s3_bucket_name" {
  default = "raytestlambda"
}

variable "lambda_runtime" {
  default = "python3.7"
}

variable "lambda_handler" {
  default = "lambda_function.lambda_handler"
}

variable "lambda_zip_location" {
  default = "output/riskviewlambda.zip"
}

variable "DM_RISK_QUEUE_TABLE_NAME" {
  default = "cvriskqueue"
}

variable "RISK_TIMER_EVENT_NAME" {
  default = "scheduled_launch_cvriskjob"
}
