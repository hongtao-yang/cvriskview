variable "region" {
  description = "The AWS region we want this bucket to live in."
  default     = "ca-central-1"
}

variable "s3_bucket_name" {
  default = "raytestlambda"
}

variable "risk_job_name" {
  default = "cvriskgluejob"
}


variable "DM_RISK_TABLE_NAME" {
  default = "cvriskresult"
}
variable "DM_RISK_QUEUE_TABLE_NAME" {
  default = "cvriskqueue"
}

variable "RISK_TIMER_EVENT_NAME" {
  default = "scheduled_launch_cvriskjob"
}

variable "RISK_GLUE_JOB_NAME" {
  default = "convertcv2risk"
}
