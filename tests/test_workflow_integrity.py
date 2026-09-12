"""Offline workflow regression tests. All MySQL/S3 access is mocked."""
from contextlib import contextmanager
import datetime as dt
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask
import jwt

from homework_service.api import api
from homework_service.jobs import JobCancelled, _expire_one_upload, _fail, _finish_job, _process
from homework_service.workflow import HomeworkWorkflow, OMITTED, WorkflowError


STUDENT = {'role': 'student', 'id': 7}
PROCTOR = {'role': 'proctor', 'id': 9}
STAMP = dt.datetime(2026, 9, 12, 12)


def submission(state='draft', **extra):
    return {'id': 1, 'homework_id': 2, 'student_id': 7, 'state': state,
            'draft_file_id': 3, 'current_file_id': None, 'reviewer_role': None,
            'reviewer_id': None, 'submitted_at_utc': STAMP, **extra}


def job(status='running', **extra):
    return {'id': 'job-1', 'submission_id': 1, 'homework_id': 2, 'student_id': 7,
            'status': status, 'stage': status, 'staging_key': 'staging/7/job.pdf',
            'created_at': STAMP, 'updated_at': STAMP, 'manual_attempts': 0, **extra}


class WorkflowIntegrityTests(unittest.TestCase):
    def setUp(self):
        for target in ('socket.socket.connect', 'mysql.connector.connect', 'homework_service.db.connection'):
            guard = patch(target, side_effect=AssertionError('Live network/DB forbidden'))
            guard.start()
            self.addCleanup(guard.stop)
        self.cursor = MagicMock()
        self.conn = MagicMock()
        @contextmanager
        def transaction():
            yield self.conn, self.cursor
        @contextmanager
        def read():
            yield self.cursor
        for target, replacement in [('homework_service.db.transaction', transaction), ('homework_service.db.read_cursor', read)]:
            guard = patch(target, replacement)
            guard.start()
            self.addCleanup(guard.stop)
        self.workflow = HomeworkWorkflow({'PDF_MAX_BYTES': 1024, 'PDF_MAX_PAGES': 35, 'S3_PRESIGN_TTL_SECONDS': 300}, MagicMock())
        self.workflow._identity = MagicMock()
        self.workflow._access_student = MagicMock()
        self.workflow._homework = MagicMock(return_value={'id': 2, 'name': 'Homework', 'deadline': dt.date(2026, 9, 12)})

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(WorkflowError) as raised:
            function(*args, **kwargs)
        self.assertEqual(raised.exception.code, code)

    def mutations(self):
        return [call.args[0] for call in self.cursor.execute.call_args_list
                if call.args[0].split()[0] in {'INSERT', 'UPDATE', 'DELETE'}]

    def test_bad_scores_never_enter_grade_transaction(self):
        for value in (None, True, False, float('nan'), float('inf'), 90.5, '90,5', 'abc', '', -1, 101):
            self.assert_code('invalid_result', self.workflow.transition, PROCTOR, 1, 'grade', result=value)
        self.assertEqual(self.mutations(), [])
        for value in (0, 100, 91, 91.0, '91'):
            self.assertEqual(self.workflow._score(value), int(value))

    def test_omitted_grade_uses_submission_day_in_moscow(self):
        self.cursor.fetchone.return_value = {'deadline': dt.date(2026, 9, 12)}
        sub = submission('in_review', submitted_at_utc=dt.datetime(2026, 9, 12, 21, 30), current_file_id=3)
        score = self.workflow._grade(self.cursor, sub, OMITTED)
        self.assertEqual(score, 95)  # 00:30 Moscow on the following day.
        self.assertTrue(any("state='graded'" in sql for sql in self.mutations()))

    def test_submit_retry_after_success_is_idempotent_without_draft(self):
        for state in ('submitted', 'in_review', 'graded'):
            self.workflow._submission = MagicMock(return_value=submission(state, draft_file_id=None, current_file_id=3))
            answer = self.workflow.submit(STUDENT, 2)
            self.assertEqual(answer['state'], state)
            self.assertEqual(answer['submitted_at_utc'], '2026-09-12T12:00:00.000Z')
        self.assertEqual(self.mutations(), [])

    def test_submit_cannot_race_active_upload_or_legacy_grade(self):
        self.workflow._submission = MagicMock(return_value=submission('revision_requested', current_file_id=4))
        self.cursor.fetchone.side_effect = [None, job()]
        self.assert_code('upload_in_progress', self.workflow.submit, STUDENT, 2)
        self.cursor.fetchone.side_effect = [{'status': 1}]
        self.assert_code('already_graded', self.workflow.submit, STUDENT, 2)
        self.assertEqual(self.mutations(), [])

    def test_new_upload_does_not_create_second_job_for_same_homework(self):
        self.workflow._submission = MagicMock(return_value=submission('uploading'))
        self.cursor.fetchone.side_effect = [None, None, job('uploading')]
        self.assert_code('upload_in_progress', self.workflow.create_upload, STUDENT, 2,
                         '06f6eea3-5f15-40d4-aa75-c052af737a43')
        self.assertEqual(self.mutations(), [])
        self.workflow.storage.presigned_upload.assert_not_called()

    def test_upload_guards_use_current_reads_after_waiting_for_submission_lock(self):
        self.workflow._submission = MagicMock(return_value=submission('none'))
        # Model an older REPEATABLE READ snapshot: ordinary SELECT sees neither
        # a concurrently committed grade nor upload, while locking SELECT does.
        def graded_after_wait():
            sql = self.cursor.execute.call_args.args[0]
            return {'status': 1} if 'homework_sessions' in sql and sql.endswith('FOR UPDATE') else None
        self.cursor.fetchone.side_effect = graded_after_wait
        self.assert_code('already_graded', self.workflow.create_upload, STUDENT, 2,
                         '06f6eea3-5f15-40d4-aa75-c052af737a43')

        def upload_after_wait():
            sql = self.cursor.execute.call_args.args[0]
            return job('uploading') if "status IN ('uploading'" in sql and sql.endswith('FOR UPDATE') else None
        self.cursor.fetchone.side_effect = upload_after_wait
        self.assert_code('upload_in_progress', self.workflow.create_upload, STUDENT, 2,
                         '06f6eea3-5f15-40d4-aa75-c052af737a43')
        self.assertEqual(self.mutations(), [])

    def test_upload_uuid_cannot_be_reused_for_another_homework(self):
        self.workflow._submission = MagicMock(return_value=submission())
        self.cursor.fetchone.side_effect = [None, job('uploading', submission_id=99)]
        self.assert_code('client_upload_id_conflict', self.workflow.create_upload, STUDENT, 2,
                         '06f6eea3-5f15-40d4-aa75-c052af737a43')

    def test_retry_cannot_replace_a_newer_job_or_a_submitted_file(self):
        self.workflow._lock_job = MagicMock(return_value=(submission('submitted'), job('failed')))
        self.assert_code('file_locked_after_submit', self.workflow.retry_job, STUDENT, 'job-1')
        self.workflow._lock_job.return_value = (submission('draft'), job('failed'))
        self.cursor.fetchone.side_effect = [None, None, {'id': 'newer-job'}]
        self.assert_code('job_superseded', self.workflow.retry_job, STUDENT, 'job-1')
        self.assertEqual(self.mutations(), [])

    def test_retry_restores_processing_and_preserves_revision_state(self):
        for state, current, expected in [('none', None, 'processing'), ('revision_requested', 4, 'revision_requested')]:
            self.cursor.reset_mock()
            self.workflow._lock_job = MagicMock(return_value=(submission(state, current_file_id=current), job('failed')))
            self.cursor.fetchone.side_effect = [None, None, None, None, job('queued', manual_attempts=1)]
            answer = self.workflow.retry_job(STUDENT, 'job-1')
            self.assertEqual(answer['status'], 'queued')
            self.assertTrue(any(call.args[1] == (expected, 1) for call in self.cursor.execute.call_args_list))

    def test_cancel_replacement_keeps_the_existing_draft(self):
        self.workflow._lock_job = MagicMock(return_value=(submission('processing'), job()))
        self.cursor.fetchone.side_effect = [None, job('cancelled')]
        self.workflow.cancel_job(STUDENT, 'job-1')
        self.assertTrue(any(call.args[1] == ('draft', 1) for call in self.cursor.execute.call_args_list))
        self.assertFalse(any('DELETE FROM homework_submission_files' in sql for sql in self.mutations()))

    def test_remove_draft_preserves_submitted_revision_and_queues_storage_delete(self):
        self.workflow._submission = MagicMock(return_value=submission('revision_requested', current_file_id=4))
        self.cursor.fetchone.side_effect = [None, None, {'object_key': 'processed/draft.pdf'}]
        answer = self.workflow.remove_draft(STUDENT, 2)
        self.assertEqual(answer['state'], 'revision_requested')
        self.assertTrue(any('homework_s3_delete_queue' in sql for sql in self.mutations()))
        deletes = [call for call in self.cursor.execute.call_args_list if 'DELETE FROM homework_submission_files' in call.args[0]]
        self.assertEqual(deletes[0].args[1], (3,))

    def test_remove_draft_is_not_allowed_during_upload_or_after_submission(self):
        self.workflow._submission = MagicMock(return_value=submission('draft'))
        self.cursor.fetchone.side_effect = [None, job()]
        self.assert_code('upload_in_progress', self.workflow.remove_draft, STUDENT, 2)
        self.workflow._submission.return_value = submission('submitted')
        self.assert_code('file_locked_after_submit', self.workflow.remove_draft, STUDENT, 2)
        self.assertEqual(self.mutations(), [])

    def test_worker_cannot_restore_submitted_or_graded_work_to_draft(self):
        for state in ('submitted', 'in_review', 'graded'):
            self.cursor.fetchone.side_effect = [submission(state), job()]
            with self.assertRaises(JobCancelled):
                _finish_job(job(), 'processed/new.pdf', {'size_bytes': 12, 'page_count': 1, 'sha256': 'abc'})
        self.assertEqual(self.mutations(), [])

    def test_late_worker_failure_does_not_resurrect_cancelled_job(self):
        self.cursor.fetchone.side_effect = [submission('none'), {'status': 'cancelled'}]
        _fail(job(), 'timeout', terminal=True)
        self.assertEqual(self.mutations(), [])

    def test_cleanup_error_does_not_delete_successfully_saved_pdf(self):
        app = MagicMock()
        app.config = {'PDF_MAX_BYTES': 1024, 'PDF_MAX_PAGES': 35}
        storage = MagicMock()
        app.extensions = {'homework_storage': storage}
        storage.delete.side_effect = RuntimeError('staging cleanup temporarily unavailable')
        with patch('homework_service.jobs.process_pdf', return_value={}), \
             patch('homework_service.jobs._progress'), \
             patch('homework_service.jobs._finish_job', return_value=None), \
             patch('homework_service.jobs._queue_delete', side_effect=RuntimeError('DB cleanup temporarily unavailable')):
            _process(app, job())
        storage.delete.assert_called_once_with('staging/7/job.pdf')

    def test_abandoned_upload_expires_and_queues_staging_cleanup(self):
        self.cursor.fetchone.return_value = {'id': 1}
        self.cursor.fetchall.return_value = [job('uploading')]
        self.assertTrue(_expire_one_upload(self.cursor, self.conn, 1800))
        self.assertTrue(any("error_code='upload_expired'" in sql for sql in self.mutations()))
        self.assertTrue(any('homework_s3_delete_queue' in sql for sql in self.mutations()))
        self.conn.commit.assert_called_once()

    def test_queue_and_archive_paginate_and_scope_proctors(self):
        row = {'id': 11, 'student_id': 7, 'homework_id': 2, 'student_name': 'Student',
               'homework_name': 'Homework', 'deadline': dt.date(2026, 9, 12),
               'submitted_at_utc': STAMP, 'reviewer_role': None, 'reviewer_id': None}
        for archive in (False, True):
            self.cursor.reset_mock()
            self.cursor.fetchone.side_effect = None
            self.cursor.fetchone.return_value = {'total': 3}
            self.cursor.fetchall.return_value = [dict(row), dict(row, id=12), dict(row, id=13)]
            answer = (self.workflow.archive(PROCTOR, {'limit': 2, 'after': 20, 'search': 'needle'}) if archive
                      else self.workflow.review_queue(PROCTOR, limit=2, after=10, search='needle'))
            self.assertEqual(len(answer['items']), 2)
            self.assertEqual(answer['total'], 3)
            self.assertTrue(answer['has_more'])
            queries = [call for call in self.cursor.execute.call_args_list if 'COUNT(*)' in call.args[0]]
            self.assertIn('p.group_id=s.group_id AND p.id=%s', queries[0].args[0])
            self.assertIn(9, queries[0].args[1])
            self.assertIn('%needle%', queries[0].args[1])

    def test_proctor_cannot_act_as_admin_reviewer(self):
        self.assert_code('not_reviewer', self.workflow._reviewer, PROCTOR,
                         submission('in_review', reviewer_role='admin', reviewer_id=1))

    def test_workspace_exposes_metadata_and_disables_submit_while_processing(self):
        sub = submission('processing', submitted_at_utc=None, revision_count=0)
        self.workflow._submission = MagicMock(return_value=sub)
        self.cursor.fetchone.side_effect = [None, job(), {'full_name': 'Student'},
                                           {'id': 3, 'size_bytes': 100, 'page_count': 2, 'created_at': STAMP}]
        answer = self.workflow.workspace(STUDENT, 2)
        self.assertEqual(answer['submission']['draft_file']['page_count'], 2)
        self.assertEqual(answer['submission']['draft_file']['size_bytes'], 100)
        self.assertTrue(answer['submission']['draft_file']['filename'].endswith('.pdf'))
        self.assertFalse(answer['permissions']['submit'])
        self.assertFalse(answer['permissions']['upload'])
        self.assertEqual(answer['active_job']['id'], 'job-1')


class WorkflowApiContractTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(TESTING=True, JWT_SECRET_KEY='offline-workflow-secret-long-enough')
        self.app.register_blueprint(api)
        self.workflow = MagicMock()
        self.workflow.transition.return_value = {'ok': True}
        self.workflow.archive.return_value = {'items': []}
        self.workflow.remove_draft.return_value = {'ok': True}
        self.app.extensions['homework_workflow'] = self.workflow
        self.client = self.app.test_client()

    def request(self, role, url, method='GET', body=None):
        now = dt.datetime.now(dt.timezone.utc)
        token = jwt.encode({'id': 7, 'role': role, 'iat': now, 'exp': now + dt.timedelta(minutes=1)},
                           self.app.config['JWT_SECRET_KEY'], algorithm='HS256')
        return self.client.open(url, method=method, json=body, headers={'Authorization': 'Bearer ' + token})

    def test_explicit_null_is_distinct_from_omitted_result(self):
        self.request('proctor', '/api/submissions/1/grade', 'POST', {})
        self.assertIs(self.workflow.transition.call_args.kwargs['result'], OMITTED)
        self.request('proctor', '/api/submissions/1/grade', 'POST', {'result': None})
        self.assertIsNone(self.workflow.transition.call_args.kwargs['result'])

    def test_archive_accepts_proctor_but_draft_delete_only_student(self):
        self.assertEqual(self.request('proctor', '/api/archive').status_code, 200)
        self.assertEqual(self.request('student', '/api/archive').status_code, 403)
        self.assertEqual(self.request('proctor', '/api/workspaces/2/draft', 'DELETE').status_code, 403)
        self.assertEqual(self.request('student', '/api/workspaces/2/draft', 'DELETE').status_code, 200)


if __name__ == '__main__':
    unittest.main()
