import unittest

from homework_service.storage import HomeworkStorage


class FakeS3:
    def __init__(self):
        self.calls = []

    def generate_presigned_post(self, **kwargs):
        self.calls.append(kwargs)
        return {'url': 'https://s3.example/upload', 'fields': {'key': kwargs['Key']}}

    def head_object(self, **kwargs):
        self.calls.append(kwargs)
        return {'ContentLength': 123, 'Metadata': {}}


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeS3()
        self.storage = HomeworkStorage({
            'S3_BUCKET': 'bucket',
            'S3_ACCESS_KEY_ID': 'key',
            'S3_SECRET_ACCESS_KEY': 'secret',
            'S3_PRESIGN_TTL_SECONDS': 300,
        }, client=self.client)

    def test_presigned_post_restricts_pdf_and_size(self):
        result = self.storage.presigned_upload('staging/7/job.pdf', 10 * 1024 * 1024, 7)
        call = self.client.calls[-1]
        self.assertEqual(result['url'], 'https://s3.example/upload')
        self.assertEqual(call['Fields']['Content-Type'], 'application/pdf')
        self.assertEqual(call['Fields']['x-amz-meta-student-id'], '7')
        self.assertIn({'x-amz-meta-student-id': '7'}, call['Conditions'])
        self.assertIn(['content-length-range', 1, 10 * 1024 * 1024], call['Conditions'])
        self.assertEqual(call['Key'], 'staging/7/job.pdf')

    def test_head_uses_exact_bucket_and_key(self):
        self.storage.head('staging/7/job.pdf')
        self.assertEqual(self.client.calls[-1], {'Bucket': 'bucket', 'Key': 'staging/7/job.pdf'})


if __name__ == '__main__':
    unittest.main()
