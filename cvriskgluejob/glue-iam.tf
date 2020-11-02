resource "aws_iam_role_policy" "cvrisk_glue_policy" {
  name = "cvrisk_glue_policy"
  role = aws_iam_role.cvrisk_glue_role.id

  policy = file("iam/glue-policy.json")
}

resource "aws_iam_role" "cvrisk_glue_role" {
  name = "cvrisk_glue_role"

  assume_role_policy = file("iam/glue-role-policy.json")
}