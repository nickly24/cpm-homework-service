"""Run the offline suite with synthetic credentials and all sockets disabled."""
import os
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
TEST_ENVIRONMENT = {
    'ENV': 'test',
    'JWT_SECRET_KEY': 'synthetic-homework-local-test-secret',
    'MYSQL_HOST': '127.0.0.1', 'MYSQL_PORT': '33077',
    'MYSQL_USER': 'synthetic', 'MYSQL_PASSWORD': '',
    'MYSQL_DATABASE': 'cpm_synthetic_test_only',
    'S3_ENDPOINT_URL': 'http://127.0.0.1:19000', 'S3_REGION': 'local',
    'S3_BUCKET': 'cpm-synthetic-test-only',
    'S3_ACCESS_KEY_ID': 'synthetic-test-key', 'S3_SECRET_ACCESS_KEY': 'synthetic-test-secret',
    'AWS_ACCESS_KEY_ID': 'synthetic-test-key', 'AWS_SECRET_ACCESS_KEY': 'synthetic-test-secret',
    'AWS_EC2_METADATA_DISABLED': 'true',
    'RUN_HOMEWORK_WORKER': 'false', 'STUDENT_DELETION_ENABLED': 'false',
    'STUDENT_PURGE_STORAGE_POLICY_VERIFIED': 'false',
}


def audit(event, args):
    if event in {'socket.connect', 'socket.getaddrinfo'}:
        raise PermissionError('Offline homework tests refuse network connections')
    if event == 'open' and args and isinstance(args[0], (str, bytes, os.PathLike)):
        path = Path(os.fsdecode(args[0]))
        if path.name == '.env' or path.name.startswith('.env.'):
            raise PermissionError('Offline homework tests refuse environment-file reads')


def main(argv=None):
    sys.dont_write_bytecode = True
    for name in tuple(os.environ):
        if name.startswith(('MYSQL_', 'S3_', 'AWS_', 'STUDENT_PURGE_', 'STUDENT_DELETION_')):
            del os.environ[name]
    os.environ.update(TEST_ENVIRONMENT)
    sys.addaudithook(audit)
    sys.path.insert(0, str(ROOT))
    os.chdir(ROOT)
    arguments = [sys.argv[0], *(sys.argv[1:] if argv is None else argv)]
    if len(arguments) == 1:
        arguments.extend(['discover', '-s', 'tests', '-v'])
    unittest.main(module=None, argv=arguments)


if __name__ == '__main__':
    main()
