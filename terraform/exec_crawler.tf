resource "aws_glue_crawler" "netflix_crawler" {
  name          = "netflix-processed-crawler"
  role          = "arn:aws:iam::362838062282:role/LabRole"
  database_name = aws_glue_catalog_database.netflix_db.name

  s3_target {
    path = "s3://netflix-analytics-williams2/processed/movies/"
  }

  s3_target {
    path = "s3://netflix-analytics-williams2/processed/ratings/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
   
}
