import psycopg2
import boto3
import csv
import os
from datetime import datetime


def lambda_handler(event, context):
    try:
        conn = psycopg2.connect(
            host=os.environ["DATABASE_HOST"],
            port=os.environ["DATABASE_PORT"],
            database=os.environ["DATABASE_NAME"],
            user=os.environ["DATABASE_USERNAME"],
            password=os.environ["DATABASE_PASSWORD"]
        )

        cursor = conn.cursor()
        query = event["query"]
        cursor.execute(query)
        rows = cursor.fetchall()

        bucket_name = os.environ["BUCKET_NAME"]

        current_time = datetime.now()
        file_name = 'public/tmp/' + current_time.strftime("%Y_%m_%d__%H_%M_%S.csv")

        local_file_path = "/tmp/local_file.csv"  # Use /tmp directory for Lambda's writable storage
        with open(local_file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([desc[0] for desc in cursor.description])
            writer.writerows(rows)

        s3_client = boto3.client("s3")
        s3_client.upload_file(local_file_path, bucket_name, file_name)

        csvfile.close()
        cursor.close()
        conn.close()

        return {
            "statusCode": 200,
            "body": file_name
        }
        # return file_name
    except Exception as e:
        raise e
