resource "aws_glue_job" "netflix_etl_job" {
  name     = "netflix-transform-job"
  role_arn = "arn:aws:iam::362838062282:role/LabRole"
  
  command {
    name            = "glueetl"
    script_location = "s3://netflix-analytics-williams2/scripts/netflix_etl.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"       = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"     = "true"
  }
  
  max_retries      = 0
  timeout          = 2880  
  worker_type      = "G.1X"
  number_of_workers = 10
  
  glue_version     = "5.0"
  
  tags = {
    Project = "Netflix-Analytics"
  }
}
