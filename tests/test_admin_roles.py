"""Offline delegated permissions tests; network and DB connections are prohibited."""
from contextlib import contextmanager
import datetime as dt
import unittest
from unittest.mock import MagicMock, patch
import jwt
from flask import Flask
from homework_service.api import api
from homework_service.admin_permissions import RULES, allowed, load_actor
from homework_service.workflow import HomeworkWorkflow, WorkflowError

SECRET = 'offline-homework-admin-test-secret'


def actor(section='review-queue', edit=False):
    return {'role': 'staff_admin', 'id': 7, 'session_version': 1,
            'permissions': {section: {'view': True, 'edit': edit}}}


class AdminRolesTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.socket.connect', 'mysql.connector.connect'):
            p = patch(target, side_effect=AssertionError('Network/DB forbidden in tests'))
            p.start()
            self.addCleanup(p.stop)
        self.app = Flask(__name__)
        self.app.config.update(TESTING=True, JWT_SECRET_KEY=SECRET)
        self.app.register_blueprint(api)
        self.workflow = MagicMock()
        self.workflow.review_queue.return_value = {'items': []}
        self.workflow.archive.return_value = {'items': []}
        self.workflow.transition.return_value = {'state': 'graded'}
        self.app.extensions['homework_workflow'] = self.workflow
        now = dt.datetime.now(dt.timezone.utc)
        self.token = jwt.encode({'id': 7, 'role': 'staff_admin', 'session_version': 1, 'iat': now, 'exp': now + dt.timedelta(minutes=1)}, SECRET, algorithm='HS256')
        self.client = self.app.test_client()

    def request(self, user, url, method='GET'):
        with patch('homework_service.admin_permissions.load_actor', return_value=user):
            return self.client.open(url, method=method, json={}, headers={'Authorization': 'Bearer ' + self.token})

    def test_all_policies_separate_view_and_edit(self):
        for endpoint, rules in RULES.items():
            for section, action in rules:
                self.assertTrue(allowed(actor(section, True), 'homework_api.' + endpoint))
                self.assertEqual(allowed(actor(section), 'homework_api.' + endpoint), action == 'view')
        self.assertFalse(allowed(actor(edit=True), 'homework_api.create_upload'))

    def test_queue_view_and_edit_use_actual_api(self):
        self.assertEqual(self.request(actor(), '/api/review-queue').status_code, 200)
        for action in ('claim', 'takeover', 'release', 'request-revision', 'grade'):
            self.assertEqual(self.request(actor(), f'/api/submissions/3/{action}', 'POST').status_code, 403)
            self.workflow.transition.assert_not_called()
        self.assertEqual(self.request(actor(edit=True), '/api/submissions/3/grade', 'POST').status_code, 200)
        self.workflow.transition.assert_called_once()

    def test_archive_cannot_edit_queue_and_queue_cannot_edit_archive(self):
        self.assertEqual(self.request(actor('homework-archive', True), '/api/submissions/3/claim', 'POST').status_code, 403)
        self.assertEqual(self.request(actor(edit=True), '/api/submissions/3/edit-grade', 'POST').status_code, 403)
        self.assertEqual(self.request(actor('homework-archive', True), '/api/submissions/3/edit-grade', 'POST').status_code, 200)
        self.assertEqual(self.request(actor('monitoring'), '/api/jobs/3/retry', 'POST').status_code, 403)

    def test_disabled_or_version_changed_actor_is_unauthorized(self):
        self.assertEqual(self.request(None, '/api/review-queue').status_code, 401)
        cursor = MagicMock()
        @contextmanager
        def read():
            yield cursor
        with patch('homework_service.db.read_cursor', read):
            for row in (None, {'is_active': 0}, {'is_active': 1, 'session_version': 2}):
                cursor.fetchone.return_value = row
                self.assertIsNone(load_actor(actor()))

    def test_view_only_queue_has_no_sql_mutations(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = {'id': 7}
        cursor.fetchall.return_value = []
        @contextmanager
        def transaction():
            yield MagicMock(), cursor
        with patch('homework_service.db.transaction', transaction):
            result = HomeworkWorkflow({}, MagicMock()).review_queue(actor())
        self.assertEqual(result['items'], [])
        self.assertTrue(all(call.args[0].startswith('SELECT') for call in cursor.execute.call_args_list))

    def test_files_enforce_section_by_submission_state(self):
        for section, state in [('review-queue', 'graded'), ('homework-archive', 'in_review'), ('review-queue', 'draft')]:
            cursor = MagicMock()
            cursor.fetchone.return_value = {'student_id': 2, 'state': state}
            @contextmanager
            def read():
                yield cursor
            workflow = HomeworkWorkflow({}, MagicMock())
            with patch('homework_service.db.read_cursor', read), patch.object(workflow, '_access_student'), self.assertRaises(WorkflowError) as error:
                workflow.file_url(actor(section), 3)
            self.assertEqual(error.exception.status, 403)
            workflow.storage.presign_download.assert_not_called()


if __name__ == '__main__':
    unittest.main()
