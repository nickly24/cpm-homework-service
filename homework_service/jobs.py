import logging
import os
import socket
import tempfile
import threading
import time
import uuid
from pathlib import Path

from . import db
from .outbox import enqueue_delete
from .pdf_pipeline import PdfRejected, process_pdf


logger = logging.getLogger(__name__)
_started = False
_start_lock = threading.Lock()
_last_heartbeat = None


class JobCancelled(RuntimeError):
    pass


def runner_status():
    age = None if _last_heartbeat is None else max(0, time.monotonic() - _last_heartbeat)
    return {'started': _started, 'heartbeat_age_seconds': round(age, 1) if age is not None else None}


def start_runner(app):
    global _started
    if not app.config.get('RUN_HOMEWORK_WORKER', True):
        return
    with _start_lock:
        if _started:
            return
        _started = True
        thread = threading.Thread(target=_loop, args=(app,), daemon=True, name='homework-pdf-runner')
        thread.start()


def _loop(app):
    global _last_heartbeat
    with app.app_context():
        while True:
            _last_heartbeat = time.monotonic()
            try:
                from .purges import publish_capability
                try:
                    publish_capability(app)
                except Exception as exc:
                    logger.warning('homework_purge_unavailable error_code=%s', type(exc).__name__.lower())
                if not run_one(app):
                    time.sleep(float(app.config['JOB_POLL_SECONDS']))
            except Exception as exc:
                logger.warning('homework_runner error_code=%s', type(exc).__name__.lower())
                time.sleep(2)


def run_one(app):
    # Keep the heavyweight PDF single-runner lock, but fence only DB claims and
    # final object writes; pure PDF rendering must not block all backend writers.
    return _run_one_fenced(app)


def _run_one_fenced(app):
    conn = db.connection()
    lock_held = False
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT GET_LOCK('cpm_homework_service_pdf_runner',0) acquired")
        lock_held = bool(cursor.fetchone()['acquired'])
        if not lock_held:
            return False

        with db.student_write_fence(conn):
            from .purges import run_one_purge
            if run_one_purge(app):
                return True

            if db.deletion_protection_active():
                _recover_delete_leases(cursor)
                conn.commit()
            cursor.execute(
                "SELECT * FROM homework_s3_delete_queue WHERE status IN ('queued','retry') "
                'AND available_at<=UTC_TIMESTAMP(6) ORDER BY id LIMIT 1 FOR UPDATE'
            )
            deletion = cursor.fetchone()
            if deletion:
                cursor.execute(
                    "UPDATE homework_s3_delete_queue SET status='running',attempts=attempts+1 WHERE id=%s",
                    (deletion['id'],),
                )
                if db.deletion_protection_active():
                    deletion['lease_owner'] = str(uuid.uuid4())
                    cursor.execute('UPDATE homework_s3_delete_queue SET lease_owner=%s,'
                                   'lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 120 SECOND) WHERE id=%s',
                                   (deletion['lease_owner'], deletion['id']))
                conn.commit()
                _delete_object(app, deletion)
                return True

            if _cleanup_one_orphan(cursor, conn):
                return True
            if _expire_one_upload(cursor, conn, max(int(app.config.get('UPLOAD_STALE_SECONDS', 1800)),
                                                    2 * int(app.config.get('S3_PRESIGN_TTL_SECONDS', 300)))):
                return True

            cursor.execute(
                "UPDATE homework_file_jobs SET status='queued',stage='queued',lease_owner=NULL,lease_expires_at=NULL "
                "WHERE status='running' AND lease_expires_at<UTC_TIMESTAMP(6)"
                + (" AND NOT EXISTS (SELECT 1 FROM student_deletion_barriers b WHERE b.student_id=homework_file_jobs.student_id)"
                   if db.deletion_protection_active() else '')
            )
            conn.commit()
            cursor.execute(
                "SELECT * FROM homework_file_jobs WHERE status IN ('queued','retry') "
                'AND available_at<=UTC_TIMESTAMP(6) '
                + ("AND NOT EXISTS (SELECT 1 FROM student_deletion_barriers b WHERE b.student_id=homework_file_jobs.student_id) "
                   if db.deletion_protection_active() else '') + 'ORDER BY created_at LIMIT 1 FOR UPDATE'
            )
            job = cursor.fetchone()
            if not job:
                conn.commit()
                return False
            worker = f'{socket.gethostname()}:{os.getpid()}:{threading.get_ident()}'
            cursor.execute(
                "UPDATE homework_file_jobs SET status='running',stage='checking',progress=15,attempts=attempts+1,"
                'lease_owner=%s,lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL %s SECOND),'
                'heartbeat_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) WHERE id=%s',
                (worker, int(app.config['JOB_STALE_SECONDS']), job['id']),
            )
            conn.commit()
        _process(app, job)
        return True
    finally:
        if lock_held:
            release = None
            try:
                release = conn.cursor()
                release.execute("SELECT RELEASE_LOCK('cpm_homework_service_pdf_runner')")
                release.fetchone()
            except Exception:
                pass
            finally:
                if release:
                    release.close()
        if cursor is not None:
            cursor.close()
        conn.close()


def _cleanup_one_orphan(cursor, conn):
    cursor.execute(
        'SELECT sub.id FROM homework_submissions sub '
        'LEFT JOIN homework h ON h.id=sub.homework_id '
        'LEFT JOIN students s ON s.id=sub.student_id '
        'WHERE (h.id IS NULL OR s.id IS NULL) '
        + ('AND NOT EXISTS (SELECT 1 FROM student_deletion_barriers b WHERE b.student_id=sub.student_id) '
           if db.deletion_protection_active() else '') + 'ORDER BY sub.id LIMIT 1 FOR UPDATE'
    )
    orphan = cursor.fetchone()
    if not orphan:
        conn.commit()
        return False
    cursor.execute(
        'SELECT object_key FROM homework_submission_files WHERE submission_id=%s',
        (orphan['id'],),
    )
    for row in cursor.fetchall():
        enqueue_delete(cursor, row['object_key'])
    cursor.execute(
        ('SELECT staging_key,processed_output_key FROM homework_file_jobs WHERE submission_id=%s'
         if db.deletion_protection_active() else 'SELECT staging_key FROM homework_file_jobs WHERE submission_id=%s AND staging_key IS NOT NULL'),
        (orphan['id'],),
    )
    for row in cursor.fetchall():
        enqueue_delete(cursor, row['staging_key'])
        if row.get('processed_output_key'):
            enqueue_delete(cursor, row['processed_output_key'])
    cursor.execute('DELETE FROM homework_submissions WHERE id=%s', (orphan['id'],))
    conn.commit()
    return True


def _expire_one_upload(cursor, conn, stale_seconds):
    cursor.execute(
        "SELECT sub.id FROM homework_submissions sub WHERE EXISTS (SELECT 1 FROM homework_file_jobs j "
        "WHERE j.submission_id=sub.id AND j.status='uploading' "
        "AND j.updated_at<DATE_SUB(UTC_TIMESTAMP(6),INTERVAL %s SECOND)) "
        + ('AND NOT EXISTS (SELECT 1 FROM student_deletion_barriers b WHERE b.student_id=sub.student_id) '
           if db.deletion_protection_active() else '') + 'ORDER BY sub.id LIMIT 1 FOR UPDATE',
        (stale_seconds,),
    )
    sub = cursor.fetchone()
    if not sub:
        conn.commit()
        return False
    cursor.execute("SELECT id,staging_key FROM homework_file_jobs WHERE submission_id=%s AND status='uploading' "
                   'AND updated_at<DATE_SUB(UTC_TIMESTAMP(6),INTERVAL %s SECOND) FOR UPDATE',
                   (sub['id'], stale_seconds))
    jobs = cursor.fetchall()
    for job in jobs:
        cursor.execute("UPDATE homework_file_jobs SET status='failed',stage='failed',error_code='upload_expired',"
                       'updated_at=UTC_TIMESTAMP(6) WHERE id=%s', (job['id'],))
        if job['staging_key']:
            enqueue_delete(cursor, job['staging_key'])
    cursor.execute("UPDATE homework_submissions SET state=CASE WHEN current_file_id IS NOT NULL THEN 'revision_requested' "
                   "WHEN draft_file_id IS NOT NULL THEN 'draft' ELSE 'none' END WHERE id=%s AND state='uploading'",
                   (sub['id'],))
    conn.commit()
    return bool(jobs)


def _recover_delete_leases(cursor):
    cursor.execute("UPDATE homework_s3_delete_queue SET status='retry',lease_owner=NULL,lease_expires_at=NULL "
                   "WHERE status='running' AND (lease_expires_at IS NULL OR lease_expires_at<=UTC_TIMESTAMP(6))")


def _delete_object(app, row):
    try:
        app.extensions['homework_storage'].delete(row['object_key'])
        if db.deletion_protection_active():
            app.extensions['homework_storage'].verify_absent(row['object_key'])
        with db.transaction() as (_, cursor):
            if db.deletion_protection_active():
                cursor.execute('INSERT IGNORE INTO homework_s3_delete_receipts(queue_id,generation,object_key,completed_at) '
                               'SELECT id,generation,object_key,UTC_TIMESTAMP(6) FROM homework_s3_delete_queue '
                               'WHERE id=%s AND lease_owner=%s AND generation=%s',
                               (row['id'], row['lease_owner'], row['generation']))
                cursor.execute("UPDATE homework_s3_delete_queue SET status='completed' ,completed_at=UTC_TIMESTAMP(6),"
                               'lease_owner=NULL,lease_expires_at=NULL,error_code=NULL WHERE id=%s AND lease_owner=%s AND generation=%s',
                               (row['id'], row['lease_owner'], row['generation']))
            else:
                cursor.execute('DELETE FROM homework_s3_delete_queue WHERE id=%s', (row['id'],))
    except Exception as exc:
        attempts = int(row['attempts'] or 0) + 1
        status = 'failed' if attempts >= 10 else 'retry'
        with db.transaction() as (_, cursor):
            cursor.execute(
                'UPDATE homework_s3_delete_queue SET status=%s,error_code=%s,'
                'available_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 MINUTE) WHERE id=%s',
                (status, type(exc).__name__.lower(), row['id']),
            )
            if db.deletion_protection_active():
                cursor.execute('UPDATE homework_s3_delete_queue SET lease_owner=NULL,lease_expires_at=NULL WHERE id=%s',
                               (row['id'],))


def _process(app, job):
    storage = app.extensions['homework_storage']
    final_key = f'processed/drafts/{uuid.uuid4()}.pdf'
    committed = False
    try:
        with tempfile.TemporaryDirectory(prefix='cpm-homework-') as folder:
            source = Path(folder) / 'source.pdf'
            output = Path(folder) / 'processed.pdf'
            storage.download_file(job['staging_key'], source)
            _progress(app, job['id'], 'optimizing', 45)
            info = process_pdf(
                source,
                output,
                int(app.config['PDF_MAX_BYTES']),
                int(app.config['PDF_MAX_PAGES']),
            )
            _progress(app, job['id'], 'saving', 75)
            with db.student_write_fence():
                if db.deletion_protection_active():
                    from .purges import reserve_output
                    reserve_output(job, final_key)
                storage.upload_file(output, final_key)
                old_key = _finish_job(job, final_key, info)
                committed = True
        try:
            storage.delete(job['staging_key'])
        except Exception:
            _queue_delete(job['staging_key'])
        if old_key:
            _queue_delete(old_key)
    except JobCancelled:
        with db.transaction() as (_, cursor):
            if db.student_is_blocked(cursor, job['student_id']):
                return  # Durable purge owns all cleanup after barrier installation.
            cursor.execute("UPDATE homework_file_jobs SET status='cancelled',stage='cancelled',lease_owner=NULL,"
                           "lease_expires_at=NULL,updated_at=UTC_TIMESTAMP(6) WHERE id=%s AND status='running'",
                           (job['id'],))
        try:
            storage.delete(final_key)
        except Exception:
            _queue_delete(final_key)
        try:
            storage.delete(job['staging_key'])
        except Exception:
            _queue_delete(job['staging_key'])
    except PdfRejected as exc:
        _fail(job, exc.code, terminal=True)
        _queue_delete(job['staging_key'], delay_hours=24)
    except Exception as exc:
        if committed:
            # Cleanup failure must not remove the PDF already referenced by a
            # successful submission. Staging also has the bucket lifecycle.
            logger.warning('homework_cleanup error_code=%s', type(exc).__name__.lower())
            return
        try:
            storage.delete(final_key)
        except Exception:
            _queue_delete(final_key)
        terminal = int(job.get('attempts') or 0) + 1 >= 3
        _fail(job, type(exc).__name__.lower(), terminal=terminal)
        if terminal:
            _queue_delete(job['staging_key'], delay_hours=24)


def _finish_job(job, final_key, info):
    old_key = None
    with db.transaction() as (_, cursor):
        if db.student_is_blocked(cursor, job['student_id']):
            raise JobCancelled('student_deletion_in_progress')
        cursor.execute('SELECT * FROM homework_submissions WHERE id=%s FOR UPDATE', (job['submission_id'],))
        submission = cursor.fetchone()
        cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s FOR UPDATE', (job['id'],))
        current_job = cursor.fetchone()
        if not current_job or current_job['status'] != 'running' or not submission:
            raise JobCancelled('job_cancelled')
        from .workflow import EDITABLE_STATES, HomeworkWorkflow
        if submission['state'] not in EDITABLE_STATES or HomeworkWorkflow._legacy_graded(cursor, submission):
            raise JobCancelled('submission_locked')
        if submission['draft_file_id'] and submission['draft_file_id'] == submission['current_file_id']:
            raise JobCancelled('file_reference_conflict')
        if submission['draft_file_id']:
            cursor.execute(
                'SELECT object_key FROM homework_submission_files WHERE id=%s',
                (submission['draft_file_id'],),
            )
            old = cursor.fetchone()
            old_key = old and old['object_key']
            if old_key:
                enqueue_delete(cursor, old_key)
        cursor.execute(
            "INSERT INTO homework_submission_files "
            "(submission_id,object_key,status,size_bytes,page_count,sha256) VALUES (%s,%s,'draft',%s,%s,%s)",
            (job['submission_id'], final_key, info['size_bytes'], info['page_count'], info['sha256']),
        )
        file_id = cursor.lastrowid
        next_state = 'revision_requested' if submission['current_file_id'] else 'draft'
        cursor.execute(
            'UPDATE homework_submissions SET draft_file_id=%s,state=%s WHERE id=%s',
            (file_id, next_state, job['submission_id']),
        )
        cursor.execute(
            "UPDATE homework_file_jobs SET status='ready',stage='ready',progress=100,result_file_id=%s,"
            'lease_owner=NULL,lease_expires_at=NULL,error_code=NULL,updated_at=UTC_TIMESTAMP(6) WHERE id=%s',
            (file_id, job['id']),
        )
        if submission['draft_file_id']:
            # Keep OLD and NEW reference targets valid for the SQL guards.
            # Historical jobs may point at the replaced draft too; clear only
            # this submission's references while that file still exists.
            cursor.execute('UPDATE homework_file_jobs SET result_file_id=NULL '
                           'WHERE submission_id=%s AND result_file_id=%s',
                           (job['submission_id'], submission['draft_file_id']))
            cursor.execute('DELETE FROM homework_submission_files WHERE id=%s', (submission['draft_file_id'],))
    return old_key


def _progress(app, job_id, stage, progress):
    with db.transaction() as (_, cursor):
        if db.deletion_protection_active():
            cursor.execute('SELECT student_id FROM homework_file_jobs WHERE id=%s', (job_id,))
            row = cursor.fetchone()
            if not row or db.student_is_blocked(cursor, row['student_id']):
                raise JobCancelled('student_deletion_in_progress')
        cursor.execute(
            'UPDATE homework_file_jobs SET stage=%s,progress=%s,heartbeat_at=UTC_TIMESTAMP(6),'
            'lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL %s SECOND),updated_at=UTC_TIMESTAMP(6) '
            'WHERE id=%s AND status=\'running\'',
            (stage, progress, int(app.config['JOB_STALE_SECONDS']), job_id),
        )


def _fail(job, code, terminal):
    status = 'failed' if terminal else 'retry'
    delay = min(30, 2 ** (int(job.get('attempts') or 0) + 1))
    with db.transaction() as (_, cursor):
        if db.student_is_blocked(cursor, job['student_id']):
            return
        cursor.execute('SELECT * FROM homework_submissions WHERE id=%s FOR UPDATE', (job['submission_id'],))
        sub = cursor.fetchone()
        cursor.execute('SELECT status FROM homework_file_jobs WHERE id=%s FOR UPDATE', (job['id'],))
        current = cursor.fetchone()
        if not sub or not current or current['status'] != 'running':
            return  # A cancelled/completed job must never be resurrected by a late failure.
        cursor.execute(
            "UPDATE homework_file_jobs SET status=%s,stage='failed',error_code=%s,lease_owner=NULL,"
            'lease_expires_at=NULL,available_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL %s SECOND),'
            'updated_at=UTC_TIMESTAMP(6) WHERE id=%s',
            (status, code, delay, job['id']),
        )
        if terminal and sub['state'] in {'none', 'uploading', 'processing', 'draft', 'revision_requested'}:
            cursor.execute(
                "UPDATE homework_submissions SET state=CASE "
                "WHEN current_file_id IS NOT NULL THEN state "
                "WHEN draft_file_id IS NOT NULL THEN 'draft' ELSE 'none' END WHERE id=%s",
                (job['submission_id'],),
            )


def _queue_delete(key, delay_hours=0):
    if not key:
        return
    with db.transaction() as (_, cursor):
        enqueue_delete(cursor, key, int(delay_hours))
