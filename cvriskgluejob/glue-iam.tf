resource "aws_iam_role_policy" "glue_policy" {
  name = "glue_policy"
  role = aws_iam_role.glue_role.id

  policy = file("iam/glue-policy.json")
}

resource "aws_iam_role" "glue_role" {
  name = "glue_role"

  assume_role_policy = file("iam/glue-role-policy.json")
}