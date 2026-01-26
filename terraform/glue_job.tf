resource "aws_glue_job" "netflix_job" {
  name     = "netflix-transform-job"
  role_arn = "arn:aws:iam::362838062282:role/LabRole"

  glue_version = "5.0"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.netflix_data.bucket}/scripts/netflix_etl.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"             = "s3://${aws_s3_bucket.netflix_data.bucket}/temp/"
    "--job-language"        = "python"
  }

  worker_type       = "G.1X"
  number_of_workers = 2

  depends_on = [aws_s3_object.glue_script]
}
