"""Durable, exact-key student purge protocol shared with the primary backend.

No network admin API, prefix deletion, or cross-store rollback. The committed
manifest survives SQL unlink and every S3 failure. Receipts are never discarded.
"""
import base64
import datetime as dt
import json
import uuid

from . import db
from .file_consistency import FileConsistencyError, KEY_OWNERS_SQL, check_file_consistency


class PurgeBlocked(RuntimeError):
    def __init__(self, code, counts=None):
        super().__init__(code)
        self.counts = counts or {}


def _require_barrier(cursor, purge):
    cursor.execute('SELECT job_id,created_at FROM student_deletion_barriers WHERE student_id=%s FOR UPDATE',
                   (purge['student_id'],))
    barrier = cursor.fetchone()
    if not barrier or barrier['job_id'] != purge['job_id']:
        raise PurgeBlocked('deletion_barrier_missing')
    return barrier


def _check_file_ownership(cursor, purge):
    # Caller holds the cross-service writer fence, including during S3 I/O.
    try:
        check_file_consistency(cursor, purge['student_id'])
    except FileConsistencyError as exc:
        raise PurgeBlocked(exc.code, exc.counts) from exc


def _check_manifest(cursor, purge, *, unlinked=False):
    """Revalidate retained keys after SQL unlink and before every S3 delete.

    Independent receipts survive their owner rows. They must never authorize a
    delete if any live source or another student's manifest now owns that key.
    All failures contain counts only, never object keys or personal rows.
    """
    counts = {}
    cursor.execute('SELECT COUNT(DISTINCT CAST(m.object_key AS BINARY)) count FROM homework_student_purge_objects m '
                   'JOIN (' + KEY_OWNERS_SQL + ') k ON k.object_key=CAST(m.object_key AS BINARY) '
                   'WHERE CAST(m.job_id AS BINARY)=CAST(%s AS BINARY) AND (k.student_id IS NULL OR k.student_id<>%s)',
                   (purge['job_id'], purge['student_id']))
    counts['manifest_foreign_owner_keys'] = int(cursor.fetchone()['count'])
    cursor.execute('SELECT COUNT(DISTINCT CAST(m.object_key AS BINARY)) count FROM homework_student_purge_objects m '
                   'JOIN homework_student_purge_objects other ON CAST(other.object_key AS BINARY)=CAST(m.object_key AS BINARY) '
                   'JOIN homework_student_purges p ON CAST(p.job_id AS BINARY)=CAST(other.job_id AS BINARY) '
                   'WHERE CAST(m.job_id AS BINARY)=CAST(%s AS BINARY) AND p.student_id<>%s', (purge['job_id'], purge['student_id']))
    counts['manifest_other_student_keys'] = int(cursor.fetchone()['count'])
    cursor.execute('SELECT COUNT(DISTINCT k.object_key) count FROM (' + KEY_OWNERS_SQL + ') k '
                   'WHERE k.student_id=%s AND NOT EXISTS (SELECT 1 FROM homework_student_purge_objects m '
                   'WHERE CAST(m.job_id AS BINARY)=CAST(%s AS BINARY) AND CAST(m.object_key AS BINARY)=k.object_key)', (purge['student_id'], purge['job_id']))
    counts['manifest_missing_live_keys'] = int(cursor.fetchone()['count'])
    if purge['inventory_completed_at']:
        # A file-only source disappears at unlink. The committed unit count is
        # then the independent check that a receipt was not lost or invented.
        cursor.execute('SELECT COUNT(*) count FROM homework_student_purge_objects '
                       'WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY)', (purge['job_id'],))
        expected = int(purge['total_units']) - 1
        counts['manifest_receipt_count_mismatch'] = abs(int(cursor.fetchone()['count']) - expected)
    if unlinked:
        cursor.execute('SELECT (SELECT COUNT(*) FROM homework_submissions WHERE student_id=%s) + '
                       '(SELECT COUNT(*) FROM homework_file_jobs WHERE student_id=%s) count',
                       (purge['student_id'], purge['student_id']))
        counts['residual_homework_rows'] = int(cursor.fetchone()['count'])
    if any(counts.values()):
        raise PurgeBlocked('homework_manifest_ownership_inconsistent', counts)


def utcnow():
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def record_upload_ownership(cursor, job, upload, config):
    """Called before returning a presign and while the global fence is held."""
    policy = json.loads(base64.b64decode(upload['fields']['policy']))
    expiration = dt.datetime.fromisoformat(policy['expiration'].replace('Z', '+00:00'))
    expires = expiration.astimezone(dt.timezone.utc).replace(tzinfo=None)
    expires += dt.timedelta(seconds=max(0, int(config.get('PRESIGN_DRAIN_SECONDS', 60))))
    cursor.execute(
        'INSERT INTO homework_object_ownership '
        '(object_key,student_id,submission_id,upload_job_id,kind,writable_until,created_at) '
        "VALUES (%s,%s,%s,%s,'staging',%s,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE "
        'writable_until=GREATEST(COALESCE(writable_until,VALUES(writable_until)),VALUES(writable_until))',
        (job['staging_key'], job['student_id'], job['submission_id'], job['id'], expires),
    )
    cursor.execute('UPDATE homework_file_jobs SET upload_expires_at=GREATEST('
                   'COALESCE(upload_expires_at,%s),%s) WHERE CAST(id AS BINARY)=CAST(%s AS BINARY)', (expires, expires, job['id']))


def reserve_output(job, key):
    """Commit ownership BEFORE sending any bytes to S3, even if finalization dies."""
    with db.transaction() as (_, cursor):
        db.require_student_writable(cursor, job['student_id'])
        cursor.execute('SELECT status FROM homework_file_jobs WHERE CAST(id AS BINARY)=CAST(%s AS BINARY) FOR UPDATE', (job['id'],))
        current = cursor.fetchone()
        if not current or current['status'] != 'running':
            from .jobs import JobCancelled
            raise JobCancelled('job_cancelled')
        cursor.execute('INSERT INTO homework_object_ownership '
                       '(object_key,student_id,submission_id,upload_job_id,kind,created_at) '
                       "VALUES (%s,%s,%s,%s,'processed',UTC_TIMESTAMP(6))", (key, job['student_id'], job['submission_id'], job['id']))
        cursor.execute('UPDATE homework_file_jobs SET processed_output_key=%s WHERE CAST(id AS BINARY)=CAST(%s AS BINARY)', (key, job['id']))


def publish_capability(app):
    # Deliberately independent of the global fence: processing a PDF can take
    # longer than the capability's freshness limit. An old heartbeat fails closed.
    if not db.deletion_enabled():
        return
    conn = db.connection()
    cursor = conn.cursor()
    try:
        try:
            if not app.config.get('STUDENT_PURGE_STORAGE_POLICY_VERIFIED', False):
                raise PurgeBlocked('storage_policy_not_verified')
            app.extensions['homework_storage'].check_ready()
        except Exception:
            cursor.execute("UPDATE homework_service_capabilities SET heartbeat_at='1970-01-01 00:00:00' "
                           "WHERE service_name='student-purge'")
            conn.commit()
            raise
        cursor.execute('INSERT INTO homework_service_capabilities(service_name,protocol_version,heartbeat_at) '
                       "VALUES ('student-purge',1,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE "
                       'protocol_version=1,heartbeat_at=UTC_TIMESTAMP(6)')
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def _inventory(cursor, purge, config):
    student_id = purge['student_id']
    barrier = _require_barrier(cursor, purge)
    _check_file_ownership(cursor, purge)
    now = utcnow()
    legacy_expiry = barrier['created_at'] + dt.timedelta(
        seconds=max(0, int(config.get('LEGACY_PRESIGN_MAX_SECONDS', 604800)))
        + max(0, int(config.get('PRESIGN_DRAIN_SECONDS', 60))))
    objects = {}

    def add(key, kind, expiry=None):
        if key:
            existing = objects.get(key)
            deadline = expiry or now
            if existing:
                deadline = max(deadline, existing['not_before'])
                if existing['kind'] == 'staging':
                    kind = 'staging'
            objects[key] = {'object_key': key, 'kind': kind, 'not_before': deadline}

    cursor.execute('SELECT object_key,kind,writable_until FROM homework_object_ownership WHERE student_id=%s',
                   (student_id,))
    for row in cursor.fetchall():
        add(row['object_key'], row['kind'], row['writable_until'] or (
            legacy_expiry if row['kind'] == 'staging' else None))
    cursor.execute('SELECT f.object_key FROM homework_submission_files f '
                   'JOIN homework_submissions s ON s.id=f.submission_id WHERE s.student_id=%s', (student_id,))
    for row in cursor.fetchall():
        add(row['object_key'], 'processed')
    cursor.execute('SELECT staging_key,processed_output_key,upload_expires_at FROM homework_file_jobs WHERE student_id=%s',
                   (student_id,))
    for row in cursor.fetchall():
        owned = objects.get(row['staging_key'])
        add(row['staging_key'], 'staging', row['upload_expires_at'] or (
            owned['not_before'] if owned else legacy_expiry))
        add(row['processed_output_key'], 'processed')

    for key, item in objects.items():
        # Fail closed on conflicting ownership, even if historical bad rows share
        # an exact key. Homework IDs may be shared; object ownership must not be.
        cursor.execute(
            'SELECT 1 FROM homework_object_ownership WHERE CAST(object_key AS BINARY)=CAST(%s AS BINARY) AND student_id<>%s '
            'UNION ALL SELECT 1 FROM homework_submission_files f JOIN homework_submissions s '
            'ON s.id=f.submission_id WHERE CAST(f.object_key AS BINARY)=CAST(%s AS BINARY) AND s.student_id<>%s '
            'UNION ALL SELECT 1 FROM homework_file_jobs WHERE CAST(staging_key AS BINARY)=CAST(%s AS BINARY) AND student_id<>%s '
            'UNION ALL SELECT 1 FROM homework_file_jobs '
            'WHERE CAST(processed_output_key AS BINARY)=CAST(%s AS BINARY) AND student_id<>%s LIMIT 1',
            (key, student_id, key, student_id, key, student_id, key, student_id),
        )
        if cursor.fetchone():
            raise PurgeBlocked('shared_object_ownership')
        cursor.execute('INSERT IGNORE INTO homework_student_purge_objects '
                       '(job_id,object_key,kind,not_before) VALUES (%s,%s,%s,%s)',
                       (purge['job_id'], key, item['kind'], item['not_before']))
    _check_manifest(cursor, purge)
    wait_until = max((item['not_before'] for item in objects.values()), default=now)
    cursor.execute('UPDATE homework_student_purges SET updated_at=UTC_TIMESTAMP(6),inventory_completed_at=UTC_TIMESTAMP(6),'
                   "total_units=%s,wait_until=%s,status='queued',lease_owner=NULL,lease_expires_at=NULL "
                   'WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY)', (len(objects) + 1, wait_until, purge['job_id']))


def _unlink(cursor, purge):
    # Manifest was committed in a previous iteration. These DELETEs never touch
    # homework definitions or another student's submission in the same homework.
    _require_barrier(cursor, purge)
    _check_file_ownership(cursor, purge)
    _check_manifest(cursor, purge)
    cursor.execute('DELETE f FROM homework_submission_files f JOIN homework_submissions s '
                   'ON s.id=f.submission_id WHERE s.student_id=%s', (purge['student_id'],))
    cursor.execute('DELETE FROM homework_file_jobs WHERE student_id=%s', (purge['student_id'],))
    cursor.execute('DELETE FROM homework_submissions WHERE student_id=%s', (purge['student_id'],))
    cursor.execute('UPDATE homework_student_purges SET updated_at=UTC_TIMESTAMP(6),sql_unlinked_at=UTC_TIMESTAMP(6),'
                   "completed_units=1,status='queued',lease_owner=NULL,lease_expires_at=NULL WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY)",
                   (purge['job_id'],))


def _advance(app, purge):
    if not app.config.get('STUDENT_PURGE_STORAGE_POLICY_VERIFIED', False):
        raise PurgeBlocked('storage_policy_not_verified')
    if not purge['inventory_completed_at']:
        with db.transaction() as (_, cursor):
            _inventory(cursor, purge, app.config)
        return
    if not purge['sql_unlinked_at']:
        with db.transaction() as (_, cursor):
            _unlink(cursor, purge)
        return
    with db.transaction() as (_, cursor):
        _require_barrier(cursor, purge)
        _check_file_ownership(cursor, purge)
        _check_manifest(cursor, purge, unlinked=True)
        cursor.execute('SELECT * FROM homework_student_purge_objects WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY) '
                       "AND status<>'completed' AND (initial_deleted_at IS NULL OR not_before<=UTC_TIMESTAMP(6)) "
                       'ORDER BY object_key LIMIT 1 FOR UPDATE', (purge['job_id'],))
        item = cursor.fetchone()
        if not item:
            cursor.execute('SELECT MIN(not_before) wake_at FROM homework_student_purge_objects '
                           "WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY) AND status<>'completed'", (purge['job_id'],))
            wake_at = cursor.fetchone()['wake_at']
            if wake_at:
                cursor.execute("UPDATE homework_student_purges SET updated_at=UTC_TIMESTAMP(6),status='waiting',available_at=%s,"
                               'lease_owner=NULL,lease_expires_at=NULL WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY)',
                               (wake_at, purge['job_id']))
            else:
                cursor.execute("UPDATE homework_student_purges SET updated_at=UTC_TIMESTAMP(6),status='completed',completed_units=total_units,"
                               'completed_at=UTC_TIMESTAMP(6),lease_owner=NULL,lease_expires_at=NULL,error_code=NULL '
                               'WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY)', (purge['job_id'],))
            return
        cursor.execute('UPDATE homework_student_purge_objects SET attempts=attempts+1 WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY) AND CAST(object_key AS BINARY)=CAST(%s AS BINARY)',
                       (purge['job_id'], item['object_key']))
    # Network I/O is outside SQL transactions, but inside the cross-service fence.
    # If the process dies here, the durable lease expires and the exact delete repeats.
    eligible_for_final_delete = item['not_before'] <= utcnow()
    app.extensions['homework_storage'].delete(item['object_key'])
    if eligible_for_final_delete:
        app.extensions['homework_storage'].verify_absent(item['object_key'])
    with db.transaction() as (_, cursor):
        _require_barrier(cursor, purge)
        _check_file_ownership(cursor, purge)
        _check_manifest(cursor, purge, unlinked=True)
        now = utcnow()
        status = 'completed' if eligible_for_final_delete else 'waiting'
        cursor.execute('UPDATE homework_student_purge_objects SET status=%s,error_code=NULL,'
                       'initial_deleted_at=COALESCE(initial_deleted_at,UTC_TIMESTAMP(6)),completed_at=%s '
                       'WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY) AND CAST(object_key AS BINARY)=CAST(%s AS BINARY)',
                       (status, now if status == 'completed' else None, purge['job_id'], item['object_key']))
        cursor.execute('SELECT COUNT(*) count FROM homework_student_purge_objects '
                       "WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY) AND status='completed'", (purge['job_id'],))
        complete = int(cursor.fetchone()['count']) + 1
        cursor.execute("UPDATE homework_student_purges SET updated_at=UTC_TIMESTAMP(6),status='queued',completed_units=%s,error_code=NULL,"
                       'lease_owner=NULL,lease_expires_at=NULL,available_at=UTC_TIMESTAMP(6) WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY)',
                       (complete, purge['job_id']))


def run_one_purge(app):
    if not db.deletion_enabled():
        return False
    with db.student_write_fence():
        return _run_one_purge_fenced(app)


def _run_one_purge_fenced(app):
    # Caller holds global fence. Each claim has a durable lease for process death.
    with db.transaction() as (_, cursor):
        cursor.execute("SELECT * FROM homework_student_purges WHERE (status IN ('queued','waiting') "
                       "AND available_at<=UTC_TIMESTAMP(6)) OR (status='running' AND "
                       '(lease_expires_at IS NULL OR lease_expires_at<=UTC_TIMESTAMP(6))) '
                       'ORDER BY created_at LIMIT 1 FOR UPDATE')
        purge = cursor.fetchone()
        if not purge:
            return False
        cursor.execute("UPDATE homework_student_purges SET updated_at=UTC_TIMESTAMP(6),status='running',attempts=attempts+1,lease_owner=%s,"
                       'lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL %s SECOND) WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY)',
                       (str(uuid.uuid4()), int(app.config.get('STUDENT_PURGE_LEASE_SECONDS', 120)), purge['job_id']))
    try:
        _advance(app, purge)
    except Exception as exc:
        if isinstance(exc, PurgeBlocked) and exc.counts:
            app.logger.warning('homework_purge_blocked code=%s counts=%s', str(exc),
                               json.dumps(exc.counts, sort_keys=True, separators=(',', ':')))
        with db.transaction() as (_, cursor):
            cursor.execute("UPDATE homework_student_purges SET updated_at=UTC_TIMESTAMP(6),status='failed',error_code=%s,"
                           'lease_owner=NULL,lease_expires_at=NULL WHERE CAST(job_id AS BINARY)=CAST(%s AS BINARY)',
                           (str(exc) if isinstance(exc, PurgeBlocked) else type(exc).__name__.lower(), purge['job_id']))
    return True
