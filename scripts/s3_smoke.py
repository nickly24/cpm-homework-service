"""Non-destructive production-bucket smoke test restricted to `_test/`."""
import os
import uuid

import boto3


def guarded_delete(client, bucket, key):
    if not key.startswith('_test/'):
        raise RuntimeError('refusing_to_delete_non_test_key')
    client.delete_object(Bucket=bucket, Key=key)


def main():
    bucket = os.environ['S3_BUCKET']
    client = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT_URL', 'https://s3.twcstorage.ru'),
        region_name=os.getenv('S3_REGION', 'ru-1'),
        aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'],
    )
    key = f'_test/homework-service/{uuid.uuid4()}.txt'
    client.put_object(Bucket=bucket, Key=key, Body=b'cpm-homework-service-smoke')
    head = client.head_object(Bucket=bucket, Key=key)
    if int(head['ContentLength']) != len(b'cpm-homework-service-smoke'):
        raise RuntimeError('unexpected_smoke_object_size')
    guarded_delete(client, bucket, key)
    print('S3 smoke passed')


if __name__ == '__main__':
    main()
