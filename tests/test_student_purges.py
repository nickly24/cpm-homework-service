"""Offline protocol tests: synthetic SQLite SQL adapter + in-memory S3.

This intentionally does not claim MySQL locking/DDL or cloud-storage integration
coverage. No production config, DB sockets or object-store clients are used.
"""
import base64
from contextlib import contextmanager, nullcontext
import datetime as dt
import json
import re
import sqlite3
import unittest
from unittest.mock import MagicMock, patch

import jwt
from flask import Flask

from homework_service import db, purges
from homework_service.outbox import enqueue_delete
from homework_service.storage import HomeworkStorage
from botocore.exceptions import ClientError
from homework_service.api import api
from homework_service.jobs import JobCancelled, _delete_object, _finish_job, _process, _recover_delete_leases
from homework_service.workflow import HomeworkWorkflow, WorkflowError


class SqlFixture:
    def __init__(self):
        self.now = dt.datetime(2026, 10, 3, 11, 0, 0)
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self.conn.create_function('utc_now', 0, lambda: str(self.now))
        self.conn.create_function('after_seconds', 1, lambda seconds: str(self.now + dt.timedelta(seconds=seconds)))
        self.conn.executescript('''
          CREATE TABLE students(id INTEGER PRIMARY KEY, full_name TEXT);
          CREATE TABLE auth_users(id INTEGER PRIMARY KEY, role TEXT, ref_id INTEGER);
          CREATE TABLE homework(id INTEGER PRIMARY KEY);
          CREATE TABLE student_deletion_barriers(student_id INTEGER PRIMARY KEY,job_id TEXT,created_at TEXT);
          CREATE TABLE homework_submissions(id INTEGER PRIMARY KEY,student_id INTEGER,homework_id INTEGER,state TEXT,
            current_file_id INTEGER,draft_file_id INTEGER);
          CREATE TABLE homework_submission_files(id INTEGER PRIMARY KEY,submission_id INTEGER,object_key TEXT);
          CREATE TABLE homework_file_jobs(id TEXT PRIMARY KEY,student_id INTEGER,submission_id INTEGER,
            staging_key TEXT,processed_output_key TEXT,upload_expires_at TEXT,status TEXT,
            homework_id INTEGER,result_file_id INTEGER);
          CREATE TABLE homework_object_ownership(object_key TEXT PRIMARY KEY,student_id INTEGER,
            submission_id INTEGER,upload_job_id TEXT,kind TEXT,writable_until TEXT,created_at TEXT);
          CREATE TABLE homework_student_purges(job_id TEXT PRIMARY KEY,student_id INTEGER,status TEXT DEFAULT 'queued',
            total_units INTEGER DEFAULT 0,completed_units INTEGER DEFAULT 0,inventory_completed_at TEXT,
            sql_unlinked_at TEXT,wait_until TEXT,lease_owner TEXT,lease_expires_at TEXT,attempts INTEGER DEFAULT 0,
            available_at TEXT,created_at TEXT,updated_at TEXT,error_code TEXT,completed_at TEXT);
          CREATE TABLE homework_student_purge_objects(job_id TEXT,object_key TEXT,kind TEXT,not_before TEXT,
            status TEXT DEFAULT 'queued',attempts INTEGER DEFAULT 0,error_code TEXT,completed_at TEXT,initial_deleted_at TEXT,
            PRIMARY KEY(job_id,object_key));
          CREATE TABLE homework_s3_delete_queue(id INTEGER PRIMARY KEY,object_key TEXT,status TEXT,attempts INTEGER,
            available_at TEXT,lease_owner TEXT,lease_expires_at TEXT,completed_at TEXT,error_code TEXT,generation INTEGER DEFAULT 1,created_at TEXT);
          CREATE UNIQUE INDEX uq_delete_key ON homework_s3_delete_queue(object_key);
          CREATE TABLE homework_s3_delete_receipts(queue_id INTEGER,generation INTEGER,object_key TEXT,completed_at TEXT,
            PRIMARY KEY(queue_id,generation));
        ''')
        self.events = []
        self.metadata_rows = None

    def execute(self, sql, params=()):
        self.events.append((sql, params))
        self.metadata_rows = None
        if 'information_schema.COLUMNS' in sql:
            self.metadata_rows = []
            for table, in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'"):
                for column in self.conn.execute('PRAGMA table_info(`' + table + '`)'):
                    self.metadata_rows.append({'TABLE_NAME': table, 'COLUMN_NAME': column[1]})
            return self
        if 'ON DUPLICATE KEY UPDATE' in sql and 'homework_s3_delete_queue' in sql:
            key, delay = params
            row = self.conn.execute('SELECT * FROM homework_s3_delete_queue WHERE object_key=?', (key,)).fetchone()
            if row is None:
                self.conn.execute("INSERT INTO homework_s3_delete_queue(object_key,status,available_at,generation,attempts) VALUES(?,'queued',?,1,0)",
                                  (key, str(self.now + dt.timedelta(hours=delay))))
            elif row['status'] in {'completed', 'failed', 'cancelled'}:
                self.conn.execute("UPDATE homework_s3_delete_queue SET status='queued',generation=generation+1,attempts=0,completed_at=NULL,lease_owner=NULL,lease_expires_at=NULL,error_code=NULL,available_at=? WHERE object_key=?",
                                  (str(self.now + dt.timedelta(hours=delay)), key))
            return self
        sql = sql.replace(' AS BINARY)', ' AS BLOB)')
        sql = sql.replace('%s', '?').replace(' FOR UPDATE', '').replace('INSERT IGNORE', 'INSERT OR IGNORE')
        sql = sql.replace('DATE_ADD(UTC_TIMESTAMP(6),INTERVAL ? SECOND)', 'after_seconds(?)')
        sql = sql.replace('DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 MINUTE)', 'after_seconds(60)')
        sql = sql.replace('UTC_TIMESTAMP(6)', 'utc_now()')
        sql = sql.replace('DELETE f FROM homework_submission_files f JOIN homework_submissions s '
                          'ON s.id=f.submission_id WHERE s.student_id=?',
                          'DELETE FROM homework_submission_files WHERE submission_id IN '
                          '(SELECT id FROM homework_submissions WHERE student_id=?)')
        params = tuple(str(value) if isinstance(value, dt.datetime) else value for value in params)
        self.cursor = self.conn.execute(sql, params)
        return self

    @staticmethod
    def row(row):
        if row is None:
            return None
        result = dict(row)
        for key, value in result.items():
            if value and (key.endswith('_at') or key in {'wait_until', 'not_before', 'writable_until', 'wake_at'}):
                result[key] = dt.datetime.fromisoformat(value)
        return result

    def fetchone(self):
        if self.metadata_rows is not None:
            return self.metadata_rows.pop(0) if self.metadata_rows else None
        return self.row(self.cursor.fetchone())

    def fetchall(self):
        if self.metadata_rows is not None:
            result, self.metadata_rows = self.metadata_rows, []
            return result
        return [self.row(row) for row in self.cursor.fetchall()]

    @contextmanager
    def transaction(self, *args, **kwargs):
        try:
            yield self.conn, self
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    @contextmanager
    def read_cursor(self, *args, **kwargs):
        yield self

    def row_for(self, table, where='', params=()):
        return self.execute('SELECT * FROM ' + table + (' WHERE ' + where if where else ''), params).fetchone()

    def rows_for(self, table):
        return self.execute('SELECT * FROM ' + table).fetchall()

    def seed(self):
        self.conn.executescript('''
          INSERT INTO students VALUES (7,'Synthetic one'),(8,'Synthetic two');
          INSERT INTO auth_users VALUES(70,'student',7),(80,'student',8);
          INSERT INTO homework VALUES(2);
          INSERT INTO homework_submissions VALUES(1,7,2,'draft',NULL,1),(2,8,2,'draft',NULL,2);
          INSERT INTO homework_submission_files VALUES(1,1,'processed/one.pdf'),(2,2,'processed/two.pdf');
        ''')
        for student, sub, prefix in [(7, 1, 'one'), (8, 2, 'two')]:
            self.conn.execute('INSERT INTO homework_file_jobs VALUES(?,?,?,?,?,?,?,?,?)',
                              (prefix, student, sub, 'staging/' + prefix, 'processed/orphan-' + prefix,
                               str(self.now), 'running', 2, None))
            for kind, key in [('staging', 'staging/' + prefix), ('processed', 'processed/orphan-' + prefix)]:
                self.conn.execute('INSERT INTO homework_object_ownership(object_key,student_id,submission_id,upload_job_id,kind,writable_until) VALUES(?,?,?,?,?,?)',
                                  (key, student, sub, prefix, kind, str(self.now) if kind == 'staging' else None))
        self.conn.commit()

    def queue(self, student=7, job='purge-7'):
        self.conn.execute('INSERT INTO student_deletion_barriers VALUES(?,?,?)', (student, job, str(self.now)))
        self.conn.execute('INSERT INTO homework_student_purges(job_id,student_id,created_at,available_at) VALUES(?,?,?,?)',
                          (job, student, str(self.now), str(self.now)))
        self.conn.commit()


class FakeStorage:
    def __init__(self):
        self.objects = {'processed/one.pdf', 'processed/two.pdf', 'staging/one', 'staging/two',
                        'processed/orphan-one', 'processed/orphan-two'}
        self.deleted = []
        self.fail = False

    def verify_absent(self, key):
        if key in self.objects:
            raise RuntimeError('object_still_present')
        return True

    def delete(self, key):
        self.deleted.append(key)
        if self.fail:
            raise OSError('synthetic failure')
        self.objects.discard(key)


class PurgeProtocolTests(unittest.TestCase):
    def setUp(self):
        self.fixture = SqlFixture()
        self.fixture.seed()
        self.storage = FakeStorage()
        self.app = Flask(__name__)
        self.app.config.update(STUDENT_DELETION_ENABLED=True, STUDENT_PURGE_STORAGE_POLICY_VERIFIED=True, TESTING=True, JWT_SECRET_KEY='synthetic-long-secret-for-tests')
        self.app.register_blueprint(api)
        self.app.extensions['homework_storage'] = self.storage
        self.app.extensions['homework_workflow'] = HomeworkWorkflow(self.app.config, self.storage)
        self.context = self.app.app_context()
        self.context.push()
        self.addCleanup(self.context.pop)
        for name, value in [('transaction', self.fixture.transaction), ('read_cursor', self.fixture.read_cursor),
                            ('student_write_fence', lambda *a, **k: nullcontext())]:
            patcher = patch.object(db, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch.object(purges, 'utcnow', lambda: self.fixture.now)
        patcher.start()
        self.addCleanup(patcher.stop)

    def advance(self, count=1):
        for _ in range(count):
            purges.run_one_purge(self.app)

    def finish(self):
        for _ in range(20):
            row = self.fixture.row_for('homework_student_purges')
            if row['status'] in {'completed', 'failed', 'waiting'}:
                return row
            self.advance()
        self.fail('purge did not converge')

    def test_manifest_is_committed_before_unlink_and_two_students_share_homework(self):
        self.fixture.queue()
        self.advance()
        self.assertEqual(len(self.fixture.rows_for('homework_student_purge_objects')), 3)
        self.assertEqual(len(self.fixture.rows_for('homework_submissions')), 2)
        self.assertEqual(self.storage.deleted, [])
        result = self.finish()
        self.assertEqual(result['status'], 'completed')
        self.assertEqual((result['completed_units'], result['total_units']), (4, 4))
        self.assertEqual(self.fixture.row_for('homework_submissions')['student_id'], 8)
        self.assertEqual(self.fixture.row_for('homework_submission_files')['object_key'], 'processed/two.pdf')
        self.assertEqual(self.fixture.row_for('homework')['id'], 2)
        self.assertEqual(self.storage.objects, {'processed/two.pdf', 'processed/orphan-two', 'staging/two'})
        # Retained receipts remain independently owned after SQL children disappear.
        self.assertEqual(len(self.fixture.rows_for('homework_student_purge_objects')), 3)

    def test_crashed_worker_lease_recovers_and_deleted_key_replays(self):
        self.fixture.queue()
        self.advance(2)
        with patch.object(purges, '_advance', side_effect=SystemExit('simulated process death')):
            with self.assertRaises(SystemExit):
                self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['status'], 'running')
        self.assertFalse(purges.run_one_purge(self.app))
        self.fixture.now += dt.timedelta(seconds=121)
        self.assertEqual(self.finish()['status'], 'completed')
        self.assertEqual(len(self.storage.deleted), 3)

    def test_crash_after_s3_delete_before_receipt_is_safely_replayed(self):
        self.fixture.queue()
        self.advance(2)
        original_delete = self.storage.delete
        def crash(key):
            original_delete(key)
            raise SystemExit('crash before SQL receipt')
        with patch.object(self.storage, 'delete', side_effect=crash):
            with self.assertRaises(SystemExit):
                self.advance()
        self.assertNotIn('processed/one.pdf', self.storage.objects)
        self.assertEqual(self.fixture.row_for('homework_student_purges')['completed_units'], 1)
        self.fixture.now += dt.timedelta(seconds=121)
        self.assertEqual(self.finish()['status'], 'completed')
        self.assertEqual(self.storage.deleted.count('processed/one.pdf'), 2)

    def test_missing_barrier_refuses_all_manifest_and_storage_work(self):
        self.fixture.queue()
        self.fixture.execute('DELETE FROM student_deletion_barriers')
        self.fixture.conn.commit()
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['error_code'], 'deletion_barrier_missing')
        self.assertEqual(len(self.fixture.rows_for('homework_submissions')), 2)
        self.assertEqual(self.storage.deleted, [])

    def test_outbox_recovers_crashed_running_row_but_not_unexpired_lease(self):
        self.fixture.execute('INSERT INTO homework_s3_delete_queue(id,object_key,status,attempts,lease_expires_at) '
                             "VALUES(1,'processed/one.pdf','running',1,?)", (str(self.fixture.now),))
        self.fixture.execute('INSERT INTO homework_s3_delete_queue(id,object_key,status,attempts,lease_expires_at) '
                             "VALUES(2,'processed/two.pdf','running',1,?)",
                             (str(self.fixture.now + dt.timedelta(seconds=60)),))
        _recover_delete_leases(self.fixture)
        self.assertEqual(self.fixture.row_for('homework_s3_delete_queue', 'id=1')['status'], 'retry')
        self.assertEqual(self.fixture.row_for('homework_s3_delete_queue', 'id=2')['status'], 'running')

    def test_storage_failure_is_visible_and_retry_keeps_manifest_and_progress(self):
        self.fixture.queue()
        self.advance(2)
        self.storage.fail = True
        self.advance()
        row = self.fixture.row_for('homework_student_purges')
        self.assertEqual(row['status'], 'failed')
        self.assertEqual(row['completed_units'], 1)
        self.assertEqual(len(self.fixture.rows_for('homework_student_purge_objects')), 3)
        self.assertFalse(purges.run_one_purge(self.app))
        self.storage.fail = False
        self.fixture.execute("UPDATE homework_student_purges SET status='queued'")
        self.fixture.conn.commit()
        self.assertEqual(self.finish()['status'], 'completed')
        self.assertEqual(self.storage.deleted.count('processed/one.pdf'), 2)

    def test_late_presigned_upload_is_redeleted_after_max_expiry(self):
        self.fixture.now += dt.timedelta(seconds=1)
        expiry = self.fixture.now + dt.timedelta(seconds=300)
        self.fixture.execute('UPDATE homework_object_ownership SET writable_until=? WHERE object_key=?',
                             (str(expiry), 'staging/one'))
        self.fixture.execute('UPDATE homework_file_jobs SET upload_expires_at=? WHERE id=?', (str(expiry), 'one'))
        self.fixture.queue()
        row = self.finish()
        self.assertEqual(row['status'], 'waiting')
        self.assertEqual((row['completed_units'], row['total_units']), (3, 4))
        self.assertNotIn('staging/one', self.storage.objects)
        self.storage.objects.add('staging/one')  # Still-valid POST arrives late.
        self.assertFalse(purges.run_one_purge(self.app))
        self.fixture.now = expiry + dt.timedelta(seconds=1)
        self.advance()
        self.assertEqual(self.finish()['status'], 'completed')
        self.assertNotIn('staging/one', self.storage.objects)
        self.assertEqual(self.storage.deleted.count('staging/one'), 2)

    def test_expiry_crossing_during_initial_delete_requires_another_final_delete(self):
        expiry = self.fixture.now + dt.timedelta(seconds=5)
        self.fixture.execute('UPDATE homework_object_ownership SET writable_until=? WHERE object_key=?',
                             (str(expiry), 'staging/one'))
        self.fixture.execute('UPDATE homework_file_jobs SET upload_expires_at=? WHERE id=?', (str(expiry), 'one'))
        self.fixture.queue()
        self.advance(4)  # inventory, unlink, two processed-key deletes
        original_delete = self.storage.delete
        def cross_expiry(key):
            original_delete(key)
            self.fixture.now = expiry + dt.timedelta(seconds=1)
            self.storage.objects.add(key)  # last accepted upload completes during initial delete
        with patch.object(self.storage, 'delete', side_effect=cross_expiry):
            self.advance()
        manifest = self.fixture.row_for('homework_student_purge_objects', 'object_key=?', ('staging/one',))
        self.assertEqual(manifest['status'], 'waiting')
        self.assertIsNone(manifest['completed_at'])
        self.assertIn('staging/one', self.storage.objects)
        self.assertEqual(self.finish()['status'], 'completed')
        self.assertNotIn('staging/one', self.storage.objects)
        self.assertEqual(self.storage.deleted.count('staging/one'), 2)

    def test_missing_barrier_or_shared_key_fails_without_unlink(self):
        self.fixture.queue()
        self.fixture.execute('UPDATE homework_submission_files SET object_key=? WHERE id=2', ('processed/one.pdf',))
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['error_code'], 'homework_file_ownership_inconsistent')
        self.assertEqual(len(self.fixture.rows_for('homework_submissions')), 2)
        self.assertEqual(len(self.fixture.rows_for('homework_student_purge_objects')), 0)
        self.assertEqual(self.storage.deleted, [])

    def test_corrupt_current_draft_and_result_links_never_create_a_manifest(self):
        self.fixture.queue()
        for sql in (
            'UPDATE homework_submissions SET current_file_id=2 WHERE id=1',
            'UPDATE homework_submissions SET draft_file_id=2 WHERE id=1',
            "UPDATE homework_file_jobs SET result_file_id=2 WHERE id='one'",
        ):
            with self.subTest(sql=sql):
                self.fixture.execute(sql)
                self.fixture.conn.commit()
                self.advance()
                result = self.fixture.row_for('homework_student_purges')
                self.assertEqual(result['error_code'], 'homework_file_ownership_inconsistent')
                self.assertEqual(len(self.fixture.rows_for('homework_student_purge_objects')), 0)
                self.assertEqual(len(self.fixture.rows_for('homework_submissions')), 2)
                self.assertEqual(self.storage.deleted, [])
                self.fixture.execute('UPDATE homework_submissions SET current_file_id=NULL,draft_file_id=id')
                self.fixture.execute('UPDATE homework_file_jobs SET result_file_id=NULL')
                self.fixture.execute("UPDATE homework_student_purges SET status='queued'")
                self.fixture.conn.commit()

    def test_rechecks_incoming_pointer_after_manifest_before_unlink(self):
        self.fixture.queue()
        self.advance()
        self.fixture.execute('UPDATE homework_submissions SET current_file_id=1 WHERE id=2')
        self.fixture.conn.commit()
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['error_code'],
                         'homework_file_ownership_inconsistent')
        self.assertEqual(len(self.fixture.rows_for('homework_submissions')), 2)
        self.assertEqual(len(self.fixture.rows_for('homework_student_purge_objects')), 3)
        self.assertEqual(self.storage.deleted, [])

    def test_missing_manifest_key_blocks_unlink_instead_of_leaving_an_object(self):
        self.fixture.queue()
        self.advance()
        self.fixture.execute("DELETE FROM homework_student_purge_objects WHERE object_key='processed/one.pdf'")
        self.fixture.conn.commit()
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['error_code'],
                         'homework_manifest_ownership_inconsistent')
        self.assertEqual(len(self.fixture.rows_for('homework_submissions')), 2)
        self.assertEqual(self.storage.deleted, [])

    def test_manifest_only_file_key_is_rechecked_after_sql_unlink(self):
        self.fixture.queue()
        self.advance(2)
        # File-only keys are no longer in live file rows or ownership backfill.
        # The retained manifest must still refuse another student's reused key.
        self.fixture.execute("UPDATE homework_submission_files SET object_key='processed/one.pdf' WHERE id=2")
        self.fixture.conn.commit()
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['error_code'],
                         'homework_file_ownership_inconsistent')
        self.assertEqual(self.storage.deleted, [])
        self.assertEqual(self.fixture.row_for('homework_submissions')['student_id'], 8)

    def test_lost_file_only_receipt_after_unlink_cannot_be_reported_complete(self):
        self.fixture.queue()
        self.advance(2)
        self.fixture.execute("DELETE FROM homework_student_purge_objects WHERE object_key='processed/one.pdf'")
        self.fixture.conn.commit()
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['error_code'],
                         'homework_manifest_ownership_inconsistent')
        self.assertIsNone(self.fixture.row_for('homework_student_purges')['completed_at'])
        self.assertEqual(self.storage.deleted, [])

    def test_barrier_is_rechecked_after_inventory_and_after_unlink(self):
        self.fixture.queue()
        self.advance()
        self.fixture.execute('DELETE FROM student_deletion_barriers')
        self.fixture.conn.commit()
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['error_code'], 'deletion_barrier_missing')
        self.assertEqual(len(self.fixture.rows_for('homework_submissions')), 2)
        self.fixture.execute('INSERT INTO student_deletion_barriers VALUES(?,?,?)',
                             (7, 'purge-7', str(self.fixture.now)))
        self.fixture.execute("UPDATE homework_student_purges SET status='queued'")
        self.fixture.conn.commit()
        self.advance()
        self.fixture.execute('DELETE FROM student_deletion_barriers')
        self.fixture.conn.commit()
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['error_code'], 'deletion_barrier_missing')
        self.assertEqual(self.storage.deleted, [])

    def test_completion_rechecks_manifest_even_when_all_receipts_are_complete(self):
        self.fixture.queue()
        self.advance(5)  # inventory, unlink, all three exact-key deletes
        self.assertEqual(self.fixture.row_for('homework_student_purges')['status'], 'queued')
        self.fixture.execute("UPDATE homework_submission_files SET object_key='processed/one.pdf' WHERE id=2")
        self.fixture.conn.commit()
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['error_code'],
                         'homework_file_ownership_inconsistent')
        self.assertIsNone(self.fixture.row_for('homework_student_purges')['completed_at'])

    def test_another_students_retained_manifest_prevents_any_unlink(self):
        self.fixture.queue()
        self.fixture.queue(student=8, job='purge-8')
        self.fixture.execute('INSERT INTO homework_student_purge_objects(job_id,object_key,kind,not_before) '
                             "VALUES('purge-8','processed/one.pdf','processed',?)", (str(self.fixture.now),))
        self.fixture.conn.commit()
        self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges', 'job_id=?', ('purge-7',))['error_code'],
                         'homework_file_ownership_inconsistent')
        self.assertEqual(len(self.fixture.rows_for('homework_submissions')), 2)
        self.assertEqual(self.storage.deleted, [])

    def test_blocked_old_jwt_and_deleted_identity_rejected(self):
        now = dt.datetime.now(dt.timezone.utc)
        token = jwt.encode({'id': 7, 'role': 'student', 'iat': now, 'exp': now + dt.timedelta(hours=1)},
                           self.app.config['JWT_SECRET_KEY'], algorithm='HS256')
        self.fixture.queue()
        response = self.app.test_client().post('/api/workspaces/2/uploads', json={'client_upload_id': 'abc'},
                                              headers={'Authorization': 'Bearer ' + token})
        self.assertEqual(response.status_code, 401)
        self.fixture.execute('DELETE FROM student_deletion_barriers')
        self.fixture.execute('DELETE FROM auth_users WHERE ref_id=7')
        response = self.app.test_client().get('/api/jobs/active', headers={'Authorization': 'Bearer ' + token})
        self.assertEqual(response.status_code, 401)

    def test_disabled_flag_never_reopens_existing_barrier(self):
        self.fixture.queue()
        self.app.config['STUDENT_DELETION_ENABLED'] = False
        with patch.object(db, '_protection_installed', True):
            with self.assertRaises(WorkflowError):
                db.require_student_writable(self.fixture, 7)
            with self.assertRaises(JobCancelled):
                _finish_job({'id': 'one', 'student_id': 7, 'submission_id': 1}, 'processed/race', {})
            self.assertFalse(purges.run_one_purge(self.app))

    def test_barrier_lands_after_auth_before_workflow_returns_error_not_500(self):
        from homework_service.auth import current_actor
        now = dt.datetime.now(dt.timezone.utc)
        token = jwt.encode({'id': 7, 'role': 'student', 'iat': now, 'exp': now + dt.timedelta(hours=1)},
                           self.app.config['JWT_SECRET_KEY'], algorithm='HS256')
        def auth_then_barrier():
            actor = current_actor()
            self.fixture.queue()
            return actor
        with patch('homework_service.auth.current_actor', side_effect=auth_then_barrier):
            response = self.app.test_client().get('/api/jobs/active', headers={'Authorization': 'Bearer ' + token})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json['error'], 'student_deletion_in_progress')

    def test_unverified_policy_gate_never_completes_or_unlinks(self):
        self.fixture.queue()
        self.app.config['STUDENT_PURGE_STORAGE_POLICY_VERIFIED'] = False
        self.advance()
        row = self.fixture.row_for('homework_student_purges')
        self.assertEqual(row['status'], 'failed')
        self.assertEqual(row['error_code'], 'storage_policy_not_verified')
        self.assertEqual(row['completed_units'], 0)
        self.assertEqual(len(self.fixture.rows_for('homework_submissions')), 2)
        self.assertEqual(self.storage.deleted, [])

    def test_object_reappears_or_head_denied_never_receives_completion(self):
        self.fixture.queue()
        self.advance(2)
        with patch.object(self.storage, 'verify_absent', side_effect=RuntimeError('object_still_present')):
            self.advance()
        self.assertEqual(self.fixture.row_for('homework_student_purges')['status'], 'failed')
        self.assertEqual(self.fixture.row_for('homework_student_purges')['completed_units'], 1)

    def test_outbox_rearms_terminal_generations_but_preserves_running(self):
        for state in ['cancelled', 'completed', 'failed', 'running']:
            self.fixture.execute('DELETE FROM homework_s3_delete_queue')
            self.fixture.execute('INSERT INTO homework_s3_delete_queue(id,object_key,status,attempts,generation,lease_owner) '
                                 'VALUES(1,?,?,9,4,?)', ('processed/key', state, 'existing-owner'))
            enqueue_delete(self.fixture, 'processed/key')
            row = self.fixture.row_for('homework_s3_delete_queue')
            self.assertEqual(row['generation'], 4 if state == 'running' else 5)
            self.assertEqual(row['status'], 'running' if state == 'running' else 'queued')
            self.assertEqual(row['lease_owner'], 'existing-owner' if state == 'running' else None)

    def test_worker_finalization_and_output_reservation_cannot_cross_barrier(self):
        self.fixture.queue()
        job = {'id': 'one', 'student_id': 7, 'submission_id': 1}
        with self.assertRaises(JobCancelled):
            _finish_job(job, 'processed/racing', {})
        with self.assertRaises(WorkflowError):
            purges.reserve_output(job, 'processed/racing')
        self.assertIsNone(self.fixture.row_for('homework_object_ownership', 'object_key=?', ('processed/racing',)))
        self.assertEqual(len(self.fixture.rows_for('homework_submission_files')), 2)

    def test_barrier_during_pdf_render_prevents_processed_upload(self):
        job = {'id': 'one', 'student_id': 7, 'submission_id': 1, 'staging_key': 'staging/one'}
        storage = MagicMock()
        self.app.extensions['homework_storage'] = storage
        self.app.config.update(PDF_MAX_BYTES=1024, PDF_MAX_PAGES=35)
        def render_then_barrier(*args):
            self.fixture.queue()
            return {}
        with patch('homework_service.jobs._progress'), \
             patch('homework_service.jobs.process_pdf', side_effect=render_then_barrier):
            _process(self.app, job)
        storage.upload_file.assert_not_called()
        self.assertEqual(len(self.fixture.rows_for('homework_object_ownership')), 4)
        self.assertEqual(self.fixture.row_for('homework_file_jobs', 'id=?', ('one',))['status'], 'running')

    def test_output_ownership_precedes_upload_and_survives_process_death(self):
        job = {'id': 'one', 'student_id': 7, 'submission_id': 1, 'staging_key': 'staging/one'}
        storage = MagicMock()
        self.app.extensions['homework_storage'] = storage
        def crash_after_upload(path, key):
            row = self.fixture.row_for('homework_object_ownership', 'object_key=?', (key,))
            self.assertEqual(row['student_id'], 7)
            self.assertEqual(self.fixture.row_for('homework_file_jobs', 'id=?', ('one',))['processed_output_key'], key)
            raise SystemExit('crash after external write')
        storage.upload_file.side_effect = crash_after_upload
        self.app.config.update(PDF_MAX_BYTES=1024, PDF_MAX_PAGES=35)
        with patch('homework_service.jobs._progress'), patch('homework_service.jobs.process_pdf', return_value={}):
            with self.assertRaises(SystemExit):
                _process(self.app, job)
        self.assertEqual(len(self.fixture.rows_for('homework_object_ownership')), 5)

    def test_outbox_success_keeps_receipt_and_is_idempotent(self):
        self.fixture.execute('INSERT INTO homework_s3_delete_queue(id,object_key,status,attempts,lease_owner) '
                             "VALUES(1,'processed/one.pdf','running',1,'lease')")
        row = self.fixture.row_for('homework_s3_delete_queue')
        _delete_object(self.app, row)
        receipt = self.fixture.row_for('homework_s3_delete_queue')
        self.assertEqual(receipt['status'], 'completed')
        self.assertIsNotNone(receipt['completed_at'])
        _delete_object(self.app, row)
        self.assertEqual(len(self.fixture.rows_for('homework_s3_delete_queue')), 1)
        enqueue_delete(self.fixture, row['object_key'])
        self.fixture.execute("UPDATE homework_s3_delete_queue SET status='running',lease_owner='lease2' WHERE id=1")
        _delete_object(self.app, self.fixture.row_for('homework_s3_delete_queue'))
        self.assertEqual(len(self.fixture.rows_for('homework_s3_delete_receipts')), 2)


class FenceAndPresignTests(unittest.TestCase):
    def test_fence_is_reentrant_releases_and_nested_transactions_reuse_connection(self):
        conn = MagicMock()
        conn.cursor.return_value.fetchone.return_value = {'acquired': 1}
        with patch.object(db, 'deletion_enabled', return_value=True), patch.object(db, 'connection', return_value=conn):
            with db.student_write_fence():
                with db.student_write_fence():
                    with db.transaction() as (actual, cursor):
                        self.assertIs(actual, conn)
            queries = [call.args[0] for call in conn.cursor.return_value.execute.call_args_list]
            self.assertEqual(sum('GET_LOCK' in sql for sql in queries), 1)
            self.assertEqual(sum('RELEASE_LOCK' in sql for sql in queries), 1)
            conn.commit.assert_called_once()
            conn.close.assert_called_once()

    def test_schema_probe_permission_denial_fails_closed_and_only_absence_is_legacy(self):
        from mysql.connector import ProgrammingError
        cursor = MagicMock()
        @contextmanager
        def fake_read():
            yield cursor
        with patch.object(db, 'deletion_enabled', return_value=False), \
             patch.object(db, '_protection_installed', False), patch.object(db, '_pool', object()), \
             patch.object(db, 'read_cursor', fake_read):
            cursor.execute.side_effect = ProgrammingError('select denied', errno=1142)
            with self.assertRaises(ProgrammingError):
                db.deletion_protection_active()
            cursor.execute.side_effect = ProgrammingError('absent', errno=1146)
            self.assertFalse(db.deletion_protection_active())
            cursor.execute.side_effect = None
            self.assertTrue(db.deletion_protection_active())
            self.assertEqual(cursor.execute.call_args.args[0], 'SELECT 1 FROM student_deletion_barriers LIMIT 0')

    def test_lock_timeout_does_not_enter_mutating_body(self):
        conn = MagicMock()
        conn.cursor.return_value.fetchone.return_value = {'acquired': 0}
        with patch.object(db, 'deletion_enabled', return_value=True), patch.object(db, 'connection', return_value=conn):
            with self.assertRaisesRegex(RuntimeError, 'student_write_fence_unavailable'):
                with db.transaction():
                    self.fail('entered fenced transaction after timeout')
            conn.commit.assert_not_called()

    def test_capability_probe_failure_invalidates_readiness_without_s3_mutation(self):
        conn = MagicMock()
        app = MagicMock()
        storage = MagicMock()
        storage.check_ready.side_effect = OSError('synthetic bucket unavailable')
        app.extensions = {'homework_storage': storage}
        app.config = {'STUDENT_PURGE_STORAGE_POLICY_VERIFIED': True}
        with patch.object(db, 'deletion_enabled', return_value=True), patch.object(db, 'connection', return_value=conn):
            with self.assertRaises(OSError):
                purges.publish_capability(app)
        self.assertIn("heartbeat_at='1970-01-01", conn.cursor.return_value.execute.call_args.args[0])
        storage.delete.assert_not_called()
        storage.upload_file.assert_not_called()

    def test_storage_absence_requires_404_and_never_accepts_403_or_existing_object(self):
        client = MagicMock()
        storage = HomeworkStorage({'S3_BUCKET': 'synthetic', 'S3_ACCESS_KEY_ID': 'fake',
                                   'S3_SECRET_ACCESS_KEY': 'fake'}, client=client)
        client.head_object.side_effect = ClientError({'Error': {'Code': 'NoSuchKey'},
                                                      'ResponseMetadata': {'HTTPStatusCode': 404}}, 'HeadObject')
        self.assertTrue(storage.verify_absent('processed/key'))
        client.head_object.side_effect = ClientError({'Error': {'Code': 'AccessDenied'},
                                                      'ResponseMetadata': {'HTTPStatusCode': 403}}, 'HeadObject')
        with self.assertRaises(ClientError):
            storage.verify_absent('processed/key')
        client.head_object.side_effect = None
        with self.assertRaisesRegex(RuntimeError, 'object_still_present'):
            storage.verify_absent('processed/key')

    def test_exact_signed_policy_expiry_is_persisted_with_drain_margin(self):
        cursor = MagicMock()
        policy = base64.b64encode(json.dumps({'expiration': '2026-10-03T12:00:00Z'}).encode()).decode()
        purges.record_upload_ownership(cursor,
            {'id': 'j', 'student_id': 7, 'submission_id': 1, 'staging_key': 'staging/one'},
            {'fields': {'policy': policy}}, {'PRESIGN_DRAIN_SECONDS': 60})
        expiry = cursor.execute.call_args_list[0].args[1][-1]
        self.assertEqual(expiry, dt.datetime(2026, 10, 3, 12, 1))


if __name__ == '__main__':
    unittest.main()
