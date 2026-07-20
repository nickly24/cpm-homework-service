import datetime as dt
import unittest

import jwt

from homework_service.app import create_app
from homework_service.auth import decode_token


SECRET = 'test-secret-at-least-for-tests'


def token(role='student', user_id=7, expires=300, algorithm='HS256'):
    now = dt.datetime.now(dt.timezone.utc)
    payload = {'role': role, 'id': user_id, 'iat': now, 'exp': now + dt.timedelta(seconds=expires)}
    return jwt.encode(payload, SECRET, algorithm=algorithm)


class FakeWorkflow:
    def workspace(self, actor, homework_id, student_id):
        return {'actor': actor, 'homework_id': homework_id, 'student_id': student_id}

    def active_jobs(self, actor):
        return {'items': [], 'actor': actor}


class AuthApiTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app(
            {'TESTING': True, 'JWT_SECRET_KEY': SECRET},
            initialize_services=False,
            run_worker=False,
        )
        self.app.extensions['homework_workflow'] = FakeWorkflow()
        self.client = self.app.test_client()

    def test_health_is_public(self):
        self.assertEqual(self.client.get('/health').status_code, 200)

    def test_valid_bearer_token(self):
        response = self.client.get(
            '/api/workspaces/3',
            headers={'Authorization': f'Bearer {token()}'},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['actor']['id'], 7)

    def test_expired_and_forged_tokens_are_rejected(self):
        expired = token(expires=-1)
        forged = jwt.encode(
            {'role': 'student', 'id': 7, 'iat': 1, 'exp': 9999999999},
            'wrong-secret',
            algorithm='HS256',
        )
        for value in (expired, forged):
            response = self.client.get('/api/workspaces/3', headers={'Authorization': f'Bearer {value}'})
            self.assertEqual(response.status_code, 401)

    def test_supervisor_and_wrong_role_are_forbidden(self):
        supervisor = self.client.get(
            '/api/workspaces/3', headers={'Authorization': f'Bearer {token("supervisor")}'},
        )
        admin_jobs = self.client.get(
            '/api/jobs/active', headers={'Authorization': f'Bearer {token("admin")}'},
        )
        self.assertEqual(supervisor.status_code, 403)
        self.assertEqual(admin_jobs.status_code, 403)

    def test_only_bearer_header_is_accepted(self):
        response = self.client.get('/api/workspaces/3', headers={'Authorization': token()})
        self.assertEqual(response.status_code, 401)

    def test_hs256_is_enforced(self):
        now = dt.datetime.now(dt.timezone.utc)
        hs384 = jwt.encode(
            {'role': 'student', 'id': 1, 'iat': now, 'exp': now + dt.timedelta(minutes=1)},
            SECRET,
            algorithm='HS384',
        )
        with self.assertRaises(jwt.InvalidAlgorithmError):
            decode_token(hs384, SECRET)

    def test_cors_origins(self):
        for origin in ('https://cpm-lms.ru', 'http://localhost:3000', 'http://127.0.0.1:3000'):
            response = self.client.options(
                '/api/workspaces/3',
                headers={'Origin': origin, 'Access-Control-Request-Method': 'GET'},
            )
            self.assertEqual(response.headers.get('Access-Control-Allow-Origin'), origin)


if __name__ == '__main__':
    unittest.main()

