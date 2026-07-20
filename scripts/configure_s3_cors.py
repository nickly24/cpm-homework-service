"""Apply browser direct-upload CORS to the configured bucket.

This script mutates bucket configuration and is intentionally never run by app startup.
Run it explicitly once with the same S3 environment variables as the service.
"""
import os

import boto3


def main():
    required = ['S3_BUCKET', 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY']
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit('Missing: ' + ', '.join(missing))
    client = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT_URL', 'https://s3.twcstorage.ru'),
        region_name=os.getenv('S3_REGION', 'ru-1'),
        aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'],
    )
    client.put_bucket_cors(
        Bucket=os.environ['S3_BUCKET'],
        CORSConfiguration={
            'CORSRules': [{
                'AllowedOrigins': [
                    'https://cpm-lms.ru',
                    'http://localhost:3000',
                    'http://127.0.0.1:3000',
                ],
                'AllowedMethods': ['POST'],
                'AllowedHeaders': ['Content-Type', 'x-amz-*'],
                'ExposeHeaders': ['ETag'],
                'MaxAgeSeconds': 3600,
            }],
        },
    )
    print('S3 CORS configured')


if __name__ == '__main__':
    main()

