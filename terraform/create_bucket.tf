provider "aws" {
  region = "us-east-1"
}


resource "aws_s3_bucket" "netflix_data" {
  bucket = "netflix-analytics-williams2"

  tags = {
    Name = "netflix-bucket"
  }
}


resource "aws_s3_object" "folders" {
  depends_on = [aws_s3_bucket.netflix_data]  
  
  for_each = {
    movies  = "processed/movies/"
    ratings = "processed/ratings/"
  }

  bucket = aws_s3_bucket.netflix_data.bucket  
  key    = each.value
  content = ""
}


resource "aws_glue_catalog_database" "netflix_db" {
  name = "netflix_database"
}
