
resource "aws_cloudwatch_event_rule" "scheduled_launch_cvriskjob" {
  name                = "scheduled_launch_cvriskjob"
  description         = "Fires every one hours"
  schedule_expression = "rate(1 hour)"
}


resource "aws_cloudwatch_event_target" "check_foo_scheduled_launch_cvriskjob" {
  rule      = aws_cloudwatch_event_rule.scheduled_launch_cvriskjob.name
  target_id = "risk_lambda_function"
  arn       = aws_lambda_function.risk_lambda_function.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_check_foo" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.risk_lambda_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduled_launch_cvriskjob.arn
}