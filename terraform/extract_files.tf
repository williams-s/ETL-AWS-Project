locals {
  raw_files = fileset("../data_raw", "*.txt")
}


resource "aws_s3_object" "raw_files" {
  for_each = { for f in local.raw_files : f => f }

  bucket = aws_s3_bucket.netflix_data.bucket
  key    = "raw/netflix/${each.key}"
  source = "../data_raw/${each.key}"
  etag   = filemd5("../data_raw/${each.key}")
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.netflix_data.bucket
  key    = "scripts/netflix_etl.py"
  source = "../scripts/netflix_etl.py"
  etag   = filemd5("../scripts/netflix_etl.py")
}
