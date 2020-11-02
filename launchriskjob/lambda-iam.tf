resource "aws_iam_role_policy" "cvrisk_launcher_policy" {
  name = "cvrisk_launcher_policy"
  role = aws_iam_role.cvrisk_launcher_policy_role.id

  policy = file("iam/lambda-policy.json")
}

resource "aws_iam_role" "cvrisk_launcher_policy_role" {
  name = "cvrisk_launcher_policy_role"

  assume_role_policy = file("iam/lambda-role-policy.json")
}