from urllib.parse import quote


class StorageNotConfigured(RuntimeError):
    pass


class HomeworkStorage:
    def __init__(self, config, client=None):
        required = ('S3_BUCKET', 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY')
        if not all(config.get(name) for name in required):
            raise StorageNotConfigured('storage_not_configured')
        self.config = config
        self.bucket = config['S3_BUCKET']
        if client is None:
            import boto3
            client = boto3.client(
                's3',
                endpoint_url=config.get('S3_ENDPOINT_URL') or None,
                region_name=config.get('S3_REGION') or None,
                aws_access_key_id=config['S3_ACCESS_KEY_ID'],
                aws_secret_access_key=config['S3_SECRET_ACCESS_KEY'],
            )
        self.client = client

    def presigned_upload(self, key, max_bytes, student_id):
        owner = str(student_id)
        return self.client.generate_presigned_post(
            Bucket=self.bucket,
            Key=key,
            Fields={
                'Content-Type': 'application/pdf',
                'x-amz-meta-student-id': owner,
            },
            Conditions=[
                {'Content-Type': 'application/pdf'},
                {'x-amz-meta-student-id': owner},
                ['content-length-range', 1, int(max_bytes)],
            ],
            ExpiresIn=int(self.config.get('S3_PRESIGN_TTL_SECONDS', 300)),
        )

    def head(self, key):
        return self.client.head_object(Bucket=self.bucket, Key=key)

    def upload_file(self, path, key):
        self.client.upload_file(
            str(path), self.bucket, key,
            ExtraArgs={'ContentType': 'application/pdf'},
        )

    def download_file(self, key, path):
        self.client.download_file(self.bucket, key, str(path))

    def delete(self, key):
        if key:
            self.client.delete_object(Bucket=self.bucket, Key=key)

    def presign_download(self, key, filename, inline=True):
        disposition = 'inline' if inline else 'attachment'
        return self.client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.bucket,
                'Key': key,
                'ResponseContentType': 'application/pdf',
                'ResponseContentDisposition': (
                    f"{disposition}; filename*=UTF-8''{quote(filename)}"
                ),
            },
            ExpiresIn=int(self.config.get('S3_PRESIGN_TTL_SECONDS', 300)),
        )

    def size_summary(self, prefix='processed/'):
        total = count = 0
        paginator = self.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get('Contents', []):
                count += 1
                total += int(item.get('Size', 0))
        return {'file_count': count, 'total_bytes': total}


def safe_pdf_filename(student_name, homework_name, submitted_at):
    def clean(value):
        forbidden = '\\/:*?"<>|\r\n'
        return ''.join(c for c in str(value) if c not in forbidden).strip() or 'Без названия'
    return f'{clean(student_name)} — {clean(homework_name)} — {submitted_at:%d.%m.%Y}.pdf'
