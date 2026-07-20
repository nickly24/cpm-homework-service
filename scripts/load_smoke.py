"""Create 300 concurrent upload-initialization requests without sending PDFs."""
import concurrent.futures
import json
import os
import uuid
from urllib import request


BASE_URL = os.environ['HOMEWORK_SERVICE_URL'].rstrip('/')
HOMEWORK_ID = int(os.environ['HOMEWORK_ID'])
TOKEN = os.environ['STUDENT_JWT']


def create_upload(_):
    body = json.dumps({'client_upload_id': str(uuid.uuid4())}).encode()
    req = request.Request(
        f'{BASE_URL}/api/workspaces/{HOMEWORK_ID}/uploads',
        data=body,
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
        method='POST',
    )
    with request.urlopen(req, timeout=20) as response:
        return response.status


with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
    statuses = list(pool.map(create_upload, range(300)))
print({status: statuses.count(status) for status in sorted(set(statuses))})
