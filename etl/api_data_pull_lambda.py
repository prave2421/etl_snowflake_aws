import json
import boto3
import requests
import awswrangler as wr
import pandas as pd
from datetime import datetime


def lambda_handler(event, context):
    # Configuration
    now = datetime.now()  # current date and time
    date_val = now.strftime("%Y-%m-%d-%H-%M-%S")  # store date time in string
    s3_key = 'api_universitiesReport/data_{}.csv'.format(date_val)
    api_url = 'http://universities.hipolabs.com/search?country=United+States'
    s3_bucket = 'dev-ecommerce-data'  # Replace with your S3 bucket name

    print(s3_key)
    # s3_key = 'universitiesReport/data.parquet'  # Replace with the desired S3 key/path for the file

    try:
        # Fetch data from the API
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        # Save data to S3
        # s3 = boto3.client('s3')
        # s3.put_object(Body=json.dumps(data), Bucket=s3_bucket, Key=s3_key)
        # Create DataFrame
        df = pd.DataFrame(data)
        wr.s3.to_csv(df=df, path=f's3://{s3_bucket}/{s3_key}')

        return {
            'statusCode': 200,
            'body': 'Data saved to S3 successfully!'
        }
    except requests.exceptions.RequestException as e:
        return {
            'statusCode': 500,
            'body': f'Error fetching data from API: {str(e)}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'An error occurred: {str(e)}'
        }
