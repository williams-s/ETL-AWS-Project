#!/bin/bash

pip install -r dataset/requirements.txt
python3 dataset/download_dataset.py

cd ../terraform
terraform apply -auto-approve

JOB_ID=$(aws glue start-job-run --job-name netflix-transform-job --query 'JobRunId' --output text)
echo "Job ID: $JOB_ID"

for i in {1..30}; do
  STATE=$(aws glue get-job-run --job-name netflix-transform-job --run-id $JOB_ID --query 'JobRun.JobRunState' --output text)
  if [ "$STATE" = "SUCCEEDED" ]; then
    break
  elif [ "$STATE" = "FAILED" ]; then
    exit 1
  fi
  sleep 30
done

aws glue start-crawler --name netflix-processed-crawler
