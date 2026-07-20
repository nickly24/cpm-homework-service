import unittest

from scripts.s3_smoke import guarded_delete


class FakeClient:
    def __init__(self):
        self.deleted = []

    def delete_object(self, **kwargs):
        self.deleted.append(kwargs)


class S3GuardTests(unittest.TestCase):
    def test_refuses_production_key(self):
        client = FakeClient()
        with self.assertRaisesRegex(RuntimeError, 'refusing_to_delete_non_test_key'):
            guarded_delete(client, 'bucket', 'processed/drafts/file.pdf')
        self.assertEqual(client.deleted, [])

    def test_allows_test_key(self):
        client = FakeClient()
        guarded_delete(client, 'bucket', '_test/homework/file.txt')
        self.assertEqual(client.deleted[0]['Key'], '_test/homework/file.txt')


if __name__ == '__main__':
    unittest.main()
