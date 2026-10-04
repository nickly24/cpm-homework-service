"""Offline ownership fixtures use audited 2026-10-03 column names.

The source DDL declares no FK for submission current/draft_file_id or job
result_file_id. These fixtures deliberately have no FKs, so orphan and crossed
links are exercised by SQL validation rather than hidden by an engine constraint.
SQLite validates selections only; this is not a claim of MySQL lock coverage.
"""
import sqlite3
import unittest

from homework_service.file_consistency import (
    FileConsistencyError, REQUIRED_COLUMNS, check_file_consistency,
)


class AuditColumnsFixture:
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE students(id INTEGER PRIMARY KEY);
            CREATE TABLE homework(id INTEGER PRIMARY KEY);
            CREATE TABLE homework_submissions(id INTEGER PRIMARY KEY,homework_id INTEGER,student_id INTEGER,
                current_file_id INTEGER,draft_file_id INTEGER);
            CREATE TABLE homework_submission_files(id INTEGER PRIMARY KEY,submission_id INTEGER,object_key TEXT);
            CREATE TABLE homework_file_jobs(id TEXT COLLATE NOCASE PRIMARY KEY,submission_id INTEGER,homework_id INTEGER,
                student_id INTEGER,staging_key TEXT,result_file_id INTEGER,processed_output_key TEXT);
            CREATE TABLE homework_object_ownership(object_key TEXT PRIMARY KEY,student_id INTEGER,
                submission_id INTEGER,upload_job_id TEXT COLLATE NOCASE);
            CREATE TABLE homework_student_purges(job_id TEXT COLLATE NOCASE PRIMARY KEY,student_id INTEGER);
            CREATE TABLE homework_student_purge_objects(job_id TEXT COLLATE NOCASE,object_key TEXT);
            INSERT INTO students VALUES(7),(8);
            INSERT INTO homework VALUES(2),(3);
            INSERT INTO homework_submissions VALUES(1,2,7,11,NULL),(2,2,8,12,NULL),(3,3,7,NULL,13);
            INSERT INTO homework_submission_files VALUES(11,1,'processed/one'),(12,2,'processed/two'),(13,3,'processed/three');
            INSERT INTO homework_file_jobs VALUES('one',1,2,7,'staging/one',11,'processed/one'),
                ('two',2,2,8,'staging/two',12,'processed/two');
            INSERT INTO homework_object_ownership VALUES('processed/one',7,1,'one'),('staging/one',7,1,'one'),
                ('processed/two',8,2,'two'),('staging/two',8,2,'two');
        """)
        self.events = []
        self.result = None

    def execute(self, sql, params=()):
        self.events.append((sql, params))
        self.result = None
        if 'information_schema.COLUMNS' in sql:
            self.result = [{'TABLE_NAME': table, 'COLUMN_NAME': col[1]}
                           for table, in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")
                           for col in self.db.execute('PRAGMA table_info(' + table + ')')]
            return
        # MySQL BINARY is a byte cast; SQLite calls that affinity BLOB.
        sql = sql.replace(' AS BINARY)', ' AS BLOB)')
        self.cursor = self.db.execute(sql.replace('%s', '?'), params)

    def fetchall(self):
        if self.result is not None:
            return self.result
        return [dict(row) for row in self.cursor.fetchall()]

    def fetchone(self):
        row = self.cursor.fetchone()
        return dict(row) if row is not None else None


class FileConsistencyTests(unittest.TestCase):
    def setUp(self):
        self.f = AuditColumnsFixture()
        self.addCleanup(self.f.db.close)

    def invalid(self, code, student_id=7):
        with self.assertRaises(FileConsistencyError) as raised:
            check_file_consistency(self.f, student_id)
        error = raised.exception
        self.assertEqual(error.code, 'homework_file_ownership_inconsistent')
        self.assertGreater(error.counts[code], 0)
        self.assertEqual(error.scope, 'global' if student_id is None else 'student')
        self.assertNotIn('processed/', str(error))
        self.assertNotIn('staging/', str(error))
        self.assertTrue(all(isinstance(count, int) for count in error.counts.values()))
        return error

    def test_consistent_two_students_share_homework_without_sharing_ownership(self):
        for student_id in (7, 8, None):
            result = check_file_consistency(self.f, student_id)
            self.assertFalse(any(result['counts'].values()))
        self.assertTrue(all(sql.startswith('SELECT') for sql, _ in self.f.events))
        self.assertEqual(self.f.db.execute('SELECT COUNT(*) FROM homework').fetchone()[0], 2)

    def test_current_and_draft_pointers_must_belong_to_exact_submission(self):
        for field in ('current_file_id', 'draft_file_id'):
            with self.subTest(field=field):
                self.f.db.execute('UPDATE homework_submissions SET ' + field + '=12 WHERE id=1')
                self.invalid('submission_' + field + '_mismatch')
                self.f.db.execute('UPDATE homework_submissions SET ' + field + '=NULL WHERE id=1')
        self.f.db.execute('UPDATE homework_submissions SET current_file_id=13 WHERE id=1')
        self.invalid('submission_current_file_id_mismatch')  # same student, another submission

    def test_incoming_other_student_pointer_into_target_file_blocks(self):
        self.f.db.execute('UPDATE homework_submissions SET current_file_id=11 WHERE id=2')
        self.invalid('submission_current_file_id_mismatch')

    def test_dangling_current_draft_and_result_are_detected_without_fk(self):
        for table, field, identity, code in (
            ('homework_submissions', 'current_file_id', 1, 'submission_current_file_id_mismatch'),
            ('homework_submissions', 'draft_file_id', 1, 'submission_draft_file_id_mismatch'),
            ('homework_file_jobs', 'result_file_id', 'one', 'job_result_file_mismatch'),
        ):
            with self.subTest(field=field):
                old = self.f.db.execute('SELECT ' + field + ' FROM ' + table + ' WHERE id=?', (identity,)).fetchone()[0]
                self.f.db.execute('UPDATE ' + table + ' SET ' + field + '=999 WHERE id=?', (identity,))
                self.invalid(code)
                self.f.db.execute('UPDATE ' + table + ' SET ' + field + '=? WHERE id=?', (old, identity))

    def test_job_student_homework_and_submission_must_agree(self):
        for field, value, original in (('student_id',8,7),('homework_id',3,2),('submission_id',999,1)):
            with self.subTest(field=field):
                self.f.db.execute('UPDATE homework_file_jobs SET ' + field + '=? WHERE id=?', (value, 'one'))
                self.invalid('job_submission_owner_mismatch')
                self.f.db.execute('UPDATE homework_file_jobs SET ' + field + '=? WHERE id=?', (original, 'one'))

    def test_outgoing_and_incoming_job_result_must_agree(self):
        self.f.db.execute("UPDATE homework_file_jobs SET result_file_id=12 WHERE id='one'")
        self.invalid('job_result_file_mismatch')
        self.f.db.execute("UPDATE homework_file_jobs SET result_file_id=11 WHERE id='one'")
        self.f.db.execute("UPDATE homework_file_jobs SET result_file_id=11 WHERE id='two'")
        self.invalid('job_result_file_mismatch')

    def test_unassignable_orphan_files_block_every_student_and_global(self):
        self.f.db.execute("INSERT INTO homework_submission_files VALUES(99,999,'orphan/key')")
        for student_id in (7,8,999,None):
            self.invalid('orphan_file_submission', student_id)

    def test_unrelated_owned_corruption_is_scoped_but_global_preflight_catches_it(self):
        self.f.db.execute('UPDATE homework_submissions SET current_file_id=999 WHERE id=2')
        self.assertFalse(any(check_file_consistency(self.f, 7)['counts'].values()))
        self.invalid('submission_current_file_id_mismatch', None)

    def test_every_key_source_participates_in_cross_owner_check(self):
        for table, field, where in (
            ('homework_submission_files','object_key','id=12'),
            ('homework_file_jobs','staging_key',"id='two'"),
            ('homework_file_jobs','processed_output_key',"id='two'"),
            ('homework_object_ownership','object_key',"object_key='staging/two'"),
        ):
            with self.subTest(table=table, field=field):
                self.f.db.execute('SAVEPOINT scenario')
                try:
                    # No uniqueness collision within one table is needed: the
                    # conflicting key is owned in a different table/source.
                    target_key = 'processed/three' if table == 'homework_object_ownership' else 'staging/one'
                    self.f.db.execute('UPDATE ' + table + ' SET ' + field + '=? WHERE ' + where, (target_key,))
                    self.invalid('shared_object_ownership')
                finally:
                    self.f.db.execute('ROLLBACK TO scenario')
                    self.f.db.execute('RELEASE scenario')

    def test_opaque_keys_preserve_case_accents_and_trailing_bytes(self):
        # Old tables use case/accent-insensitive collations. Different S3 bytes
        # must not merge just because their textual SQL values compare equal.
        for key in ('processed/ONE', 'processed/one ', 'processed/óne'):
            with self.subTest(key=key):
                self.f.db.execute("UPDATE homework_file_jobs SET staging_key=? WHERE id='two'", (key,))
                self.assertFalse(any(check_file_consistency(self.f, 7)['counts'].values()))
        self.f.db.execute("UPDATE homework_file_jobs SET staging_key='processed/one' WHERE id='two'")
        self.invalid('shared_object_ownership')

    def test_cross_table_opaque_job_ids_use_byte_identity(self):
        # A retained UUID with different bytes is not a link to another job,
        # even if the underlying legacy text column has a NOCASE collation.
        self.f.db.execute("INSERT INTO homework_object_ownership VALUES('historical/id',7,NULL,'TWO')")
        self.assertFalse(any(check_file_consistency(self.f, 7)['counts'].values()))
        self.f.db.execute("UPDATE homework_object_ownership SET upload_job_id='two' WHERE object_key='historical/id'")
        self.invalid('ownership_job_mismatch')

    def test_retained_manifest_ownership_survives_old_file_unlink(self):
        self.f.db.execute("INSERT INTO homework_student_purges VALUES('older-purge',8)")
        self.f.db.execute("INSERT INTO homework_student_purge_objects VALUES('older-purge','processed/one')")
        self.invalid('shared_object_ownership')
        self.f.db.execute("UPDATE homework_student_purge_objects SET object_key='processed/ONE'")
        self.assertFalse(any(check_file_consistency(self.f, 7)['counts'].values()))

    def test_unassignable_manifest_and_nonexact_uuid_block_all_scopes(self):
        self.f.db.execute("INSERT INTO homework_student_purges VALUES('older-purge',8)")
        self.f.db.execute("INSERT INTO homework_student_purge_objects VALUES('OLDER-PURGE','historical/key')")
        for student_id in (7,8,None):
            self.invalid('orphan_purge_manifest', student_id)

    def test_same_student_shared_key_cannot_hide_another_submission(self):
        self.f.db.execute("UPDATE homework_submission_files SET object_key='processed/one' WHERE id=13")
        self.invalid('shared_object_submission')

    def test_receipt_existing_parents_must_agree_but_unlinked_parents_are_valid(self):
        self.f.db.execute("UPDATE homework_object_ownership SET submission_id=2 WHERE object_key='processed/one'")
        self.invalid('ownership_submission_mismatch')
        self.f.db.execute("UPDATE homework_object_ownership SET submission_id=1,upload_job_id='two' WHERE object_key='processed/one'")
        self.invalid('ownership_job_mismatch')
        self.f.db.execute("DELETE FROM homework_object_ownership WHERE object_key='processed/one'")
        self.f.db.execute("INSERT INTO homework_object_ownership VALUES('historical/key',7,555,'removed-job')")
        self.assertFalse(any(check_file_consistency(self.f, 7)['counts'].values()))

    def test_missing_audited_or_migration_column_never_skips_a_check(self):
        for table, field in (
            ('homework_submissions','current_file_id'),('homework_submissions','draft_file_id'),
            ('homework_file_jobs','homework_id'),('homework_file_jobs','result_file_id'),
            ('homework_file_jobs','processed_output_key'),('homework_object_ownership','submission_id'),
        ):
            with self.subTest(table=table, field=field):
                columns = {name: set(cols) for name, cols in REQUIRED_COLUMNS.items()}
                columns[table].remove(field)
                with self.assertRaises(FileConsistencyError) as raised:
                    check_file_consistency(self.f, 7, columns)
                self.assertEqual(raised.exception.code, 'homework_file_schema_incomplete')
                self.assertEqual(raised.exception.counts, {'missing_required_columns': 1})

    def test_blank_key_and_missing_homework_or_student_are_not_clean(self):
        self.f.db.execute("UPDATE homework_submission_files SET object_key='' WHERE id=11")
        self.invalid('empty_file_key')
        self.f.db.execute('DELETE FROM homework WHERE id=2')
        self.invalid('orphan_submission_homework')
        self.f.db.execute('DELETE FROM students WHERE id=7')
        self.invalid('orphan_submission_student')


if __name__ == '__main__':
    unittest.main()
