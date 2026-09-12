import datetime as dt
from decimal import Decimal, InvalidOperation
import uuid
from zoneinfo import ZoneInfo

from . import db
from .storage import safe_pdf_filename


MOSCOW = ZoneInfo('Europe/Moscow')
ACTIVE_JOB_STATUSES = ('uploading', 'queued', 'running', 'retry')
EDITABLE_STATES = {'none', 'uploading', 'processing', 'draft', 'revision_requested'}
OMITTED = object()


class WorkflowError(RuntimeError):
    def __init__(self, code, status=400, details=None):
        super().__init__(code)
        self.code = code
        self.status = status
        self.details = details


def _iso(value):
    if not value:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def _job_json(row):
    keys = (
        'id', 'homework_id', 'status', 'stage', 'progress', 'error_code',
        'attempts', 'manual_attempts', 'created_at', 'updated_at',
    )
    result = {key: row.get(key) for key in keys}
    result['created_at'] = _iso(result.get('created_at'))
    result['updated_at'] = _iso(result.get('updated_at'))
    return result


class HomeworkWorkflow:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage

    @staticmethod
    def _active_job(cursor, submission_id, exclude_id=None, lock=False):
        cursor.execute(
            "SELECT * FROM homework_file_jobs WHERE submission_id=%s "
            "AND status IN ('uploading','queued','running','retry') "
            + ('AND id<>%s ' if exclude_id else '') + 'ORDER BY created_at DESC LIMIT 1'
            + (' FOR UPDATE' if lock else ''),
            (submission_id, exclude_id) if exclude_id else (submission_id,),
        )
        return cursor.fetchone()

    @staticmethod
    def _legacy_graded(cursor, sub):
        # Current read is essential after waiting for the submission lock: an
        # earlier identity query may already have opened a REPEATABLE READ snapshot.
        cursor.execute('SELECT status FROM homework_sessions WHERE homework_id=%s AND student_id=%s FOR UPDATE',
                       (sub['homework_id'], sub['student_id']))
        legacy = cursor.fetchone()
        return bool(legacy and legacy['status'])

    def _editable(self, cursor, sub):
        if not sub or sub['state'] not in EDITABLE_STATES:
            raise WorkflowError('file_locked_after_submit', 409)
        if self._legacy_graded(cursor, sub):
            raise WorkflowError('already_graded', 409)

    @staticmethod
    def _lock_job(cursor, job_id):
        # Always lock submission before job, as submit/create_upload do.
        cursor.execute('SELECT submission_id FROM homework_file_jobs WHERE id=%s', (job_id,))
        reference = cursor.fetchone()
        if not reference:
            raise WorkflowError('job_not_found', 404)
        cursor.execute('SELECT * FROM homework_submissions WHERE id=%s FOR UPDATE', (reference['submission_id'],))
        sub = cursor.fetchone()
        cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s FOR UPDATE', (job_id,))
        job = cursor.fetchone()
        if not sub or not job:
            raise WorkflowError('job_not_found', 404)
        return sub, job

    @staticmethod
    def _restored_state(sub):
        if sub['state'] == 'revision_requested' or sub.get('current_file_id'):
            return 'revision_requested'
        return 'draft' if sub.get('draft_file_id') else 'none'

    @staticmethod
    def _suggested_score(deadline, submitted_at=None):
        stamp = submitted_at or dt.datetime.now(dt.timezone.utc)
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=dt.timezone.utc)
        day = stamp.astimezone(MOSCOW).date()
        return 100 if deadline is None else max(0, 100 - 5 * max(0, (day - deadline).days))

    @staticmethod
    def _reviewer_label(cursor, role, reviewer_id):
        table = {'proctor': 'proctors', 'admin': 'admins', 'staff_admin': 'admin_role_users'}.get(role)
        if not reviewer_id or not table:
            return None
        cursor.execute(f'SELECT full_name FROM {table} WHERE id=%s', (reviewer_id,))
        row = cursor.fetchone()
        return row.get('full_name') if row else None

    @staticmethod
    def _file_metadata(cursor, file_id, sub, homework_name, student_name):
        if not file_id:
            return None
        cursor.execute('SELECT id,size_bytes,page_count,created_at FROM homework_submission_files WHERE id=%s', (file_id,))
        row = cursor.fetchone()
        if not row:
            return None
        stamp = sub.get('submitted_at_utc') or row['created_at']
        stamp = stamp.replace(tzinfo=dt.timezone.utc).astimezone(MOSCOW)
        return {**row, 'created_at': _iso(row['created_at']),
                'filename': safe_pdf_filename(student_name, homework_name, stamp)}

    @staticmethod
    def _identity(cursor, actor):
        tables = {'student': 'students', 'proctor': 'proctors', 'admin': 'admins', 'staff_admin': 'admin_role_users'}
        table = tables.get(actor.get('role'))
        if not table:
            raise WorkflowError('forbidden', 403)
        cursor.execute(f'SELECT id FROM {table} WHERE id=%s', (actor['id'],))
        if not cursor.fetchone():
            raise WorkflowError('account_not_found', 401)

    @classmethod
    def _access_student(cls, cursor, actor, student_id):
        cls._identity(cursor, actor)
        if actor['role'] == 'student':
            if int(actor['id']) != int(student_id):
                raise WorkflowError('forbidden', 403)
            return
        if actor['role'] in {'admin', 'staff_admin'}:
            return
        cursor.execute(
            'SELECT 1 FROM proctors p JOIN students s ON s.group_id=p.group_id '
            'WHERE p.id=%s AND s.id=%s',
            (actor['id'], student_id),
        )
        if not cursor.fetchone():
            raise WorkflowError('student_not_in_current_group', 403)

    @staticmethod
    def _homework(cursor, homework_id, published=False):
        cursor.execute('SELECT id,name,deadline,published FROM homework WHERE id=%s', (homework_id,))
        row = cursor.fetchone()
        if not row:
            raise WorkflowError('homework_not_found', 404)
        if published and not row.get('published'):
            raise WorkflowError('homework_not_published', 403)
        return row

    @staticmethod
    def _submission(cursor, homework_id, student_id, create=False, lock=False):
        suffix = ' FOR UPDATE' if lock else ''
        cursor.execute(
            'SELECT * FROM homework_submissions WHERE homework_id=%s AND student_id=%s' + suffix,
            (homework_id, student_id),
        )
        row = cursor.fetchone()
        if not row and create:
            cursor.execute(
                "INSERT INTO homework_submissions (homework_id,student_id,state) VALUES (%s,%s,'none') "
                'ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)',
                (homework_id, student_id),
            )
            cursor.execute(
                'SELECT * FROM homework_submissions WHERE homework_id=%s AND student_id=%s' + suffix,
                (homework_id, student_id),
            )
            row = cursor.fetchone()
        return row

    def workspace(self, actor, homework_id, student_id=None):
        student_id = int(student_id or actor['id'])
        with db.read_cursor() as cursor:
            self._access_student(cursor, actor, student_id)
            homework = self._homework(cursor, homework_id, actor['role'] == 'student')
            submission = self._submission(cursor, homework_id, student_id)
            if actor['role'] == 'staff_admin':
                from .admin_permissions import has_permission
                section = 'homework-archive' if submission and submission['state'] == 'graded' else 'review-queue'
                if not has_permission(actor, section):
                    raise WorkflowError('forbidden', 403)
            cursor.execute(
                'SELECT id,status,result,date_pass FROM homework_sessions '
                'WHERE homework_id=%s AND student_id=%s',
                (homework_id, student_id),
            )
            legacy = cursor.fetchone()
            active_job = self._active_job(cursor, submission['id']) if submission else None
            cursor.execute('SELECT full_name FROM students WHERE id=%s', (student_id,))
            student = cursor.fetchone()
            visible = None
            if submission:
                private = actor['role'] != 'student' and submission['state'] in {
                    'none', 'uploading', 'processing', 'draft',
                }
                if not private:
                    visible = {
                        'id': submission['id'],
                        'state': submission['state'],
                        'submitted_at_utc': _iso(submission['submitted_at_utc']),
                        'revision_count': submission['revision_count'],
                        'revision_comment': submission.get('revision_comment'),
                        'has_file': bool(submission['current_file_id']),
                        'has_draft': bool(submission['draft_file_id']) if actor['role'] == 'student' else False,
                        'reviewer': (
                            {'role': submission['reviewer_role'], 'id': submission['reviewer_id'],
                             'full_name': self._reviewer_label(cursor, submission['reviewer_role'], submission['reviewer_id'])}
                            if submission['reviewer_id'] else None
                        ),
                        'current_file': self._file_metadata(cursor, submission['current_file_id'], submission, homework['name'], student['full_name']),
                        'draft_file': self._file_metadata(cursor, submission['draft_file_id'], submission, homework['name'], student['full_name']) if actor['role'] == 'student' else None,
                    }
            state = submission['state'] if submission else 'none'
            graded = bool(legacy and legacy['status'])
            return {
                'homework': homework,
                'legacy_result': legacy,
                'suggested_score': self._suggested_score(homework['deadline'], submission.get('submitted_at_utc') if submission else None),
                'active_job': _job_json(active_job) if active_job and actor['role'] == 'student' else None,
                'limits': {'max_bytes': int(self.config.get('PDF_MAX_BYTES', 10 * 1024 * 1024)),
                           'max_pages': int(self.config.get('PDF_MAX_PAGES', 35)), 'poll_after_seconds': 10},
                'submission': visible or {'state': 'none', 'has_file': False, 'has_draft': False},
                'permissions': {
                    'upload': actor['role'] == 'student' and not graded and not active_job and state in {
                        'none', 'uploading', 'processing', 'draft', 'revision_requested',
                    },
                    'submit': actor['role'] == 'student' and not graded and not active_job and bool(
                        submission and submission['draft_file_id']
                    ) and state in {'draft', 'revision_requested'},
                    'remove_draft': actor['role'] == 'student' and not graded and not active_job and bool(
                        submission and submission['draft_file_id']
                    ) and state in {'draft', 'revision_requested'},
                },
            }

    @staticmethod
    def _valid_uuid(value, error):
        try:
            return str(uuid.UUID(str(value)))
        except (ValueError, TypeError, AttributeError) as exc:
            raise WorkflowError(error) from exc

    def create_upload(self, actor, homework_id, client_upload_id):
        if actor['role'] != 'student':
            raise WorkflowError('forbidden', 403)
        client_upload_id = self._valid_uuid(client_upload_id, 'invalid_client_upload_id')
        with db.transaction() as (_, cursor):
            self._identity(cursor, actor)
            self._homework(cursor, homework_id, published=True)
            submission = self._submission(cursor, homework_id, actor['id'], create=True, lock=True)
            if self._legacy_graded(cursor, submission):
                raise WorkflowError('already_graded', 409)
            if submission['state'] not in {
                'none', 'uploading', 'processing', 'draft', 'revision_requested',
            }:
                raise WorkflowError('file_locked_after_submit', 409)
            cursor.execute(
                'SELECT * FROM homework_file_jobs WHERE student_id=%s AND client_upload_id=%s FOR UPDATE',
                (actor['id'], client_upload_id),
            )
            job = cursor.fetchone()
            if job and job['submission_id'] != submission['id']:
                raise WorkflowError('client_upload_id_conflict', 409)
            if not job:
                active = self._active_job(cursor, submission['id'], lock=True)
                if active:
                    raise WorkflowError('upload_in_progress', 409, {'job': _job_json(active)})
                job_id = str(uuid.uuid4())
                key = f'staging/{actor["id"]}/{job_id}.pdf'
                cursor.execute(
                    'INSERT INTO homework_file_jobs '
                    '(id,client_upload_id,submission_id,homework_id,student_id,status,stage,progress,staging_key,'
                    'available_at,created_at,updated_at) '
                    "VALUES (%s,%s,%s,%s,%s,'uploading','uploading',0,%s,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))",
                    (job_id, client_upload_id, submission['id'], homework_id, actor['id'], key),
                )
                cursor.execute(
                    "UPDATE homework_submissions SET state='uploading' WHERE id=%s "
                    "AND state NOT IN ('revision_requested')",
                    (submission['id'],),
                )
                cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s', (job_id,))
                job = cursor.fetchone()
            elif job['status'] != 'uploading':
                return {'job': _job_json(job), 'upload': None, 'poll_after_seconds': 10}
        upload = self.storage.presigned_upload(
            job['staging_key'], self.config['PDF_MAX_BYTES'], actor['id']
        )
        return {
            'job': _job_json(job),
            'upload': {'method': 'POST', 'url': upload['url'], 'fields': upload['fields']},
            'max_bytes': self.config['PDF_MAX_BYTES'],
            'poll_after_seconds': 10,
        }

    def complete_upload(self, actor, job_id):
        with db.transaction() as (_, cursor):
            sub, job = self._lock_job(cursor, job_id)
            self._access_student(cursor, actor, job['student_id'])
            if actor['role'] != 'student':
                raise WorkflowError('forbidden', 403)
            if job['status'] != 'uploading':
                return _job_json(job)
            self._editable(cursor, sub)
            try:
                head = self.storage.head(job['staging_key'])
            except Exception as exc:
                raise WorkflowError('staging_object_not_found', 409) from exc
            size = int(head.get('ContentLength') or 0)
            metadata = head.get('Metadata') or {}
            if size < 1 or size > int(self.config['PDF_MAX_BYTES']):
                raise WorkflowError('source_too_large', 413)
            if metadata.get('student-id') != str(actor['id']):
                raise WorkflowError('staging_owner_mismatch', 403)
            cursor.execute(
                "UPDATE homework_file_jobs SET status='queued',stage='queued',progress=5,source_size_bytes=%s,"
                'available_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) WHERE id=%s',
                (size, job_id),
            )
            cursor.execute(
                "UPDATE homework_submissions SET state='processing' WHERE id=%s "
                "AND state NOT IN ('revision_requested')",
                (job['submission_id'],),
            )
            cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s', (job_id,))
            return _job_json(cursor.fetchone())

    def job(self, actor, job_id):
        with db.read_cursor() as cursor:
            cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s', (job_id,))
            job = cursor.fetchone()
            if not job:
                raise WorkflowError('job_not_found', 404)
            self._access_student(cursor, actor, job['student_id'])
            return _job_json(job)

    def active_jobs(self, actor):
        if actor['role'] != 'student':
            raise WorkflowError('forbidden', 403)
        with db.read_cursor() as cursor:
            self._identity(cursor, actor)
            marks = ','.join(['%s'] * len(ACTIVE_JOB_STATUSES))
            cursor.execute(
                f'SELECT * FROM homework_file_jobs WHERE student_id=%s AND status IN ({marks}) '
                'ORDER BY created_at LIMIT 30',
                (actor['id'], *ACTIVE_JOB_STATUSES),
            )
            items = [_job_json(row) for row in cursor.fetchall()]
            return {'items': items, 'poll_after_seconds': 10, 'polling_required': bool(items)}

    def cancel_job(self, actor, job_id):
        key = None
        with db.transaction() as (_, cursor):
            sub, job = self._lock_job(cursor, job_id)
            self._access_student(cursor, actor, job['student_id'])
            if actor['role'] != 'student':
                raise WorkflowError('forbidden', 403)
            if job['status'] == 'cancelled':
                return _job_json(job)
            if job['status'] in {'ready', 'failed'}:
                raise WorkflowError('job_not_cancellable', 409)
            self._editable(cursor, sub)
            key = job['staging_key']
            cursor.execute(
                "UPDATE homework_file_jobs SET status='cancelled',stage='cancelled',"
                'lease_expires_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) WHERE id=%s',
                (job_id,),
            )
            cursor.execute(
                'UPDATE homework_submissions SET state=%s WHERE id=%s',
                (self._restored_state(sub), job['submission_id']),
            )
            cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s', (job_id,))
            result = _job_json(cursor.fetchone())
        try:
            self.storage.delete(key)
        except Exception:
            self.enqueue_delete(key)
        return result

    def retry_job(self, actor, job_id):
        with db.transaction() as (_, cursor):
            sub, job = self._lock_job(cursor, job_id)
            self._access_student(cursor, actor, job['student_id'])
            if job['status'] != 'failed':
                if job['status'] in {'queued', 'retry', 'running', 'ready'} and int(job['manual_attempts']) > 0:
                    return _job_json(job)
                raise WorkflowError('job_not_retryable', 409)
            self._editable(cursor, sub)
            active = self._active_job(cursor, sub['id'], exclude_id=job_id, lock=True)
            if active:
                raise WorkflowError('upload_in_progress', 409, {'job': _job_json(active)})
            cursor.execute('SELECT id FROM homework_file_jobs WHERE submission_id=%s AND created_at>%s LIMIT 1 FOR UPDATE',
                           (sub['id'], job['created_at']))
            if cursor.fetchone():
                raise WorkflowError('job_superseded', 409)
            if int(job['manual_attempts']) >= 3:
                raise WorkflowError('manual_retry_limit', 409)
            cursor.execute('SELECT status FROM homework_s3_delete_queue WHERE object_key=%s FOR UPDATE',
                           (job['staging_key'],))
            deletion = cursor.fetchone()
            if deletion and deletion['status'] in {'running', 'failed'}:
                raise WorkflowError('staging_object_expired', 409)
            try:
                self.storage.head(job['staging_key'])
            except Exception as exc:
                raise WorkflowError('staging_object_expired', 409) from exc
            cursor.execute(
                'DELETE FROM homework_s3_delete_queue WHERE object_key=%s',
                (job['staging_key'],),
            )
            cursor.execute(
                "UPDATE homework_file_jobs SET status='queued',stage='queued',progress=5,error_code=NULL,"
                'manual_attempts=manual_attempts+1,available_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) '
                'WHERE id=%s',
                (job_id,),
            )
            cursor.execute('UPDATE homework_submissions SET state=%s WHERE id=%s',
                           ('revision_requested' if sub['current_file_id'] else 'processing', sub['id']))
            cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s', (job_id,))
            return _job_json(cursor.fetchone())

    def submit(self, actor, homework_id):
        if actor['role'] != 'student':
            raise WorkflowError('forbidden', 403)
        with db.transaction() as (_, cursor):
            self._identity(cursor, actor)
            self._homework(cursor, homework_id, published=True)
            sub = self._submission(cursor, homework_id, actor['id'], lock=True)
            if sub and sub['state'] in {'submitted', 'in_review', 'graded'} and sub['current_file_id']:
                return {'state': sub['state'], 'submitted_at_utc': _iso(sub['submitted_at_utc'])}
            self._editable(cursor, sub)
            if self._active_job(cursor, sub['id'], lock=True):
                raise WorkflowError('upload_in_progress', 409)
            if not sub['draft_file_id']:
                raise WorkflowError('draft_not_ready', 409)
            if sub['state'] not in {'draft', 'revision_requested'}:
                raise WorkflowError('invalid_state', 409)
            if sub['current_file_id']:
                cursor.execute('SELECT object_key FROM homework_submission_files WHERE id=%s', (sub['current_file_id'],))
                old = cursor.fetchone()
                if old:
                    cursor.execute(
                        "INSERT IGNORE INTO homework_s3_delete_queue (object_key,status,available_at) "
                        "VALUES (%s,'queued',UTC_TIMESTAMP(6))",
                        (old['object_key'],),
                    )
                cursor.execute('DELETE FROM homework_submission_files WHERE id=%s', (sub['current_file_id'],))
            now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
            cursor.execute("UPDATE homework_submission_files SET status='current' WHERE id=%s", (sub['draft_file_id'],))
            cursor.execute(
                "UPDATE homework_submissions SET state='submitted',current_file_id=draft_file_id,draft_file_id=NULL,"
                'submitted_at_utc=%s,reviewer_role=NULL,reviewer_id=NULL,revision_comment=NULL WHERE id=%s',
                (now, sub['id']),
            )
            return {'state': 'submitted', 'submitted_at_utc': _iso(now)}

    def remove_draft(self, actor, homework_id):
        if actor['role'] != 'student':
            raise WorkflowError('forbidden', 403)
        with db.transaction() as (_, cursor):
            self._identity(cursor, actor)
            self._homework(cursor, homework_id, published=True)
            sub = self._submission(cursor, homework_id, actor['id'], lock=True)
            if not sub:
                return {'ok': True, 'state': 'none'}
            self._editable(cursor, sub)
            if self._active_job(cursor, sub['id'], lock=True):
                raise WorkflowError('upload_in_progress', 409)
            if sub['draft_file_id']:
                cursor.execute('SELECT object_key FROM homework_submission_files WHERE id=%s', (sub['draft_file_id'],))
                file = cursor.fetchone()
                if file:
                    cursor.execute("INSERT IGNORE INTO homework_s3_delete_queue (object_key,status,available_at) VALUES (%s,'queued',UTC_TIMESTAMP(6))",
                                   (file['object_key'],))
                cursor.execute('DELETE FROM homework_submission_files WHERE id=%s', (sub['draft_file_id'],))
            state = 'revision_requested' if sub['current_file_id'] else 'none'
            cursor.execute('UPDATE homework_submissions SET draft_file_id=NULL,state=%s WHERE id=%s', (state, sub['id']))
            return {'ok': True, 'state': state}

    @staticmethod
    def _page_values(limit, after):
        try:
            return min(max(int(limit), 1), 100), max(int(after), 0)
        except (TypeError, ValueError, OverflowError) as exc:
            raise WorkflowError('invalid_pagination') from exc

    def _decorate_review_items(self, cursor, items):
        reviewers = {}
        for item in items:
            stamp = item.get('submitted_at_utc')
            item['suggested_score'] = self._suggested_score(item['deadline'], stamp)
            item['submitted_at_utc'] = _iso(stamp)
            item['filename'] = safe_pdf_filename(item['student_name'], item['homework_name'],
                                                (stamp or dt.datetime.now(dt.timezone.utc)).replace(tzinfo=dt.timezone.utc).astimezone(MOSCOW))
            reviewer = (item.get('reviewer_role'), item.get('reviewer_id'))
            if reviewer not in reviewers:
                reviewers[reviewer] = self._reviewer_label(cursor, *reviewer)
            item['reviewer_name'] = reviewers[reviewer]
        return items

    def review_queue(self, actor, state=None, limit=50, after=0, search=None):
        if actor['role'] not in {'proctor', 'admin', 'staff_admin'}:
            raise WorkflowError('forbidden', 403)
        limit, after = self._page_values(limit, after)
        states = [state] if state and state != 'all' else ['submitted', 'in_review', 'revision_requested']
        if any(value not in {'submitted', 'in_review', 'revision_requested'} for value in states):
            raise WorkflowError('invalid_state')
        with db.transaction() as (_, cursor):
            self._identity(cursor, actor)
            from .admin_permissions import has_permission
            if actor['role'] != 'staff_admin' or has_permission(actor, 'review-queue', 'edit'):
                cursor.execute(
                    "UPDATE homework_submissions sub JOIN students s ON s.id=sub.student_id "
                    "LEFT JOIN proctors current_p ON current_p.group_id=s.group_id "
                    "SET sub.state='submitted',sub.reviewer_role=NULL,sub.reviewer_id=NULL "
                    "WHERE sub.state='in_review' AND sub.reviewer_role='proctor' "
                    "AND (current_p.id IS NULL OR current_p.id<>sub.reviewer_id)"
                )
            marks = ','.join(['%s'] * len(states))
            where = [f'sub.state IN ({marks})']
            params = list(states)
            if actor['role'] == 'proctor':
                where.append('EXISTS (SELECT 1 FROM proctors p WHERE p.group_id=s.group_id AND p.id=%s)')
                params.append(actor['id'])
            if search and str(search).strip():
                where.append('(s.full_name LIKE %s OR h.name LIKE %s OR g.name LIKE %s)')
                params.extend(['%' + str(search).strip()[:200] + '%'] * 3)
            joins = (' FROM homework_submissions sub JOIN homework h ON h.id=sub.homework_id '
                     'JOIN students s ON s.id=sub.student_id LEFT JOIN `groups` g ON g.id=s.group_id '
                     'LEFT JOIN homework_submission_files f ON f.id=sub.current_file_id ')
            base = joins + 'WHERE ' + ' AND '.join(where)
            cursor.execute('SELECT COUNT(*) total' + base, tuple(params))
            total = int((cursor.fetchone() or {}).get('total', 0))
            cursor.execute(
                'SELECT sub.id,sub.homework_id,sub.student_id,sub.state,sub.submitted_at_utc,'
                'sub.reviewer_role,sub.reviewer_id,sub.revision_comment,h.name homework_name,h.deadline,'
                's.full_name student_name,g.name group_name,f.size_bytes,f.page_count' + base +
                ' AND sub.id>%s ORDER BY sub.id LIMIT %s', tuple(params + [after, limit + 1]),
            )
            rows = cursor.fetchall()
            items = self._decorate_review_items(cursor, rows[:limit])
            return {'items': items, 'next_cursor': items[-1]['id'] if items else None,
                    'total': total, 'has_more': len(rows) > limit}

    def transition(self, actor, submission_id, action, message=None, result=OMITTED):
        if actor['role'] not in {'proctor', 'admin', 'staff_admin'}:
            raise WorkflowError('forbidden', 403)
        if action in {'grade', 'edit-grade'} and result is not OMITTED:
            result = self._score(result)
        with db.transaction() as (_, cursor):
            cursor.execute('SELECT * FROM homework_submissions WHERE id=%s FOR UPDATE', (submission_id,))
            sub = cursor.fetchone()
            if not sub:
                raise WorkflowError('submission_not_found', 404)
            self._access_student(cursor, actor, sub['student_id'])
            score = None
            if action == 'claim':
                if sub['state'] == 'in_review' and sub['reviewer_role'] == actor['role'] and int(sub['reviewer_id'] or 0) == int(actor['id']):
                    return {'ok': True, 'result': None}
                if sub['state'] != 'submitted':
                    raise WorkflowError('invalid_state', 409)
                cursor.execute(
                    "UPDATE homework_submissions SET state='in_review',reviewer_role=%s,reviewer_id=%s WHERE id=%s",
                    (actor['role'], actor['id'], submission_id),
                )
            elif action == 'takeover':
                if actor['role'] not in {'admin', 'staff_admin'} or sub['state'] != 'in_review':
                    raise WorkflowError('invalid_state', 409)
                cursor.execute(
                    "UPDATE homework_submissions SET reviewer_role=%s,reviewer_id=%s WHERE id=%s",
                    (actor['role'], actor['id'], submission_id),
                )
            elif action == 'release':
                if sub['state'] == 'submitted':
                    return {'ok': True, 'result': None}
                self._reviewer(actor, sub)
                cursor.execute(
                    "UPDATE homework_submissions SET state='submitted',reviewer_role=NULL,reviewer_id=NULL WHERE id=%s",
                    (submission_id,),
                )
            elif action == 'request-revision':
                if sub['state'] == 'revision_requested' and sub.get('revision_comment') == (message or '').strip():
                    return {'ok': True, 'result': None}
                self._reviewer(actor, sub)
                comment = (message or '').strip()
                if not comment:
                    raise WorkflowError('revision_message_required', 409)
                if len(comment) > 1000:
                    raise WorkflowError('revision_message_too_long')
                cursor.execute(
                    "UPDATE homework_submissions SET state='revision_requested',reviewer_role=NULL,reviewer_id=NULL,"
                    'revision_count=revision_count+1,revision_comment=%s WHERE id=%s',
                    (comment, submission_id),
                )
            elif action == 'grade':
                if sub['state'] == 'graded':
                    cursor.execute(
                        'SELECT result FROM homework_sessions WHERE homework_id=%s AND student_id=%s',
                        (sub['homework_id'], sub['student_id']),
                    )
                    existing = cursor.fetchone()
                    if existing and (result is OMITTED or int(existing['result']) == self._score(result)):
                        return {'ok': True, 'result': int(existing['result'])}
                    raise WorkflowError('already_graded', 409)
                self._reviewer(actor, sub)
                score = self._grade(cursor, sub, result)
            elif action == 'edit-grade':
                if sub['state'] != 'graded':
                    raise WorkflowError('invalid_state', 409)
                score = self._score(result)
                cursor.execute(
                    'UPDATE homework_sessions SET result=%s WHERE homework_id=%s AND student_id=%s',
                    (score, sub['homework_id'], sub['student_id']),
                )
            elif action == 'resubmit':
                if sub['state'] != 'graded' and not (sub['state'] == 'none' and not sub['current_file_id'] and not sub['draft_file_id']):
                    raise WorkflowError('invalid_state', 409)
                if sub['state'] == 'none' and not sub['current_file_id'] and not sub['draft_file_id']:
                    return {'ok': True, 'result': None}
                if self._active_job(cursor, submission_id, lock=True):
                    raise WorkflowError('upload_in_progress', 409)
                cursor.execute('SELECT object_key FROM homework_submission_files WHERE submission_id=%s', (submission_id,))
                for file_row in cursor.fetchall():
                    cursor.execute(
                        "INSERT IGNORE INTO homework_s3_delete_queue (object_key,status,available_at) "
                        "VALUES (%s,'queued',UTC_TIMESTAMP(6))",
                        (file_row['object_key'],),
                    )
                cursor.execute('DELETE FROM homework_submission_files WHERE submission_id=%s', (submission_id,))
                cursor.execute(
                    "UPDATE homework_submissions SET state='none',draft_file_id=NULL,current_file_id=NULL,"
                    'reviewer_role=NULL,reviewer_id=NULL,submitted_at_utc=NULL,revision_comment=NULL WHERE id=%s',
                    (submission_id,),
                )
                cursor.execute(
                    'INSERT INTO homework_sessions (homework_id,student_id,status,result,date_pass) '
                    'VALUES (%s,%s,0,0,NULL) ON DUPLICATE KEY UPDATE status=0,result=0,date_pass=NULL',
                    (sub['homework_id'], sub['student_id']),
                )
            else:
                raise WorkflowError('unknown_action', 404)
            return {'ok': True, 'result': score}

    @staticmethod
    def _reviewer(actor, sub):
        if sub['state'] != 'in_review':
            raise WorkflowError('invalid_state', 409)
        if actor['role'] not in {'admin', 'staff_admin'} and (sub['reviewer_role'] != actor['role'] or int(sub['reviewer_id'] or 0) != int(actor['id'])):
            raise WorkflowError('not_reviewer', 409)

    @staticmethod
    def _score(value):
        if value is None or value is OMITTED or isinstance(value, bool):
            raise WorkflowError('invalid_result')
        try:
            score = Decimal(str(value))
            if not score.is_finite() or score != score.to_integral_value() or not 0 <= score <= 100:
                raise WorkflowError('invalid_result')
            return int(score)
        except (InvalidOperation, TypeError, ValueError, OverflowError) as exc:
            raise WorkflowError('invalid_result') from exc

    def _grade(self, cursor, sub, requested):
        cursor.execute('SELECT deadline FROM homework WHERE id=%s', (sub['homework_id'],))
        deadline = cursor.fetchone()['deadline']
        submitted_at = sub['submitted_at_utc']
        if not submitted_at:
            raise WorkflowError('submission_timestamp_missing', 409)
        submitted_date = submitted_at.replace(tzinfo=dt.timezone.utc).astimezone(MOSCOW).date()
        suggested = 100 if deadline is None else max(0, 100 - 5 * max(0, (submitted_date - deadline).days))
        score = suggested if requested is OMITTED else self._score(requested)
        cursor.execute(
            'INSERT INTO homework_sessions (homework_id,student_id,status,result,date_pass) '
            'VALUES (%s,%s,1,%s,%s) ON DUPLICATE KEY UPDATE status=1,result=VALUES(result),date_pass=VALUES(date_pass)',
            (sub['homework_id'], sub['student_id'], score, submitted_date),
        )
        cursor.execute(
            "UPDATE homework_submissions SET state='graded',reviewer_role=NULL,reviewer_id=NULL,revision_comment=NULL WHERE id=%s",
            (sub['id'],),
        )
        cursor.execute("UPDATE homework_submission_files SET status='final' WHERE id=%s", (sub['current_file_id'],))
        return score

    def file_url(self, actor, submission_id, draft=False, download=False):
        with db.read_cursor() as cursor:
            cursor.execute('SELECT * FROM homework_submissions WHERE id=%s', (submission_id,))
            sub = cursor.fetchone()
            if not sub:
                raise WorkflowError('submission_not_found', 404)
            self._access_student(cursor, actor, sub['student_id'])
            if draft and actor['role'] != 'student':
                raise WorkflowError('draft_private', 403)
            if actor['role'] == 'staff_admin':
                from .admin_permissions import has_permission
                if sub['state'] not in {'submitted', 'in_review', 'revision_requested', 'graded'}:
                    raise WorkflowError('draft_private', 403)
                section = 'homework-archive' if sub['state'] == 'graded' else 'review-queue'
                if not has_permission(actor, section):
                    raise WorkflowError('forbidden', 403)
            file_id = sub['draft_file_id'] if draft else sub['current_file_id']
            if not file_id:
                raise WorkflowError('file_not_found', 404)
            cursor.execute('SELECT object_key,size_bytes,page_count,created_at FROM homework_submission_files WHERE id=%s', (file_id,))
            file_row = cursor.fetchone()
            if not file_row:
                raise WorkflowError('file_not_found', 404)
            cursor.execute(
                'SELECT s.full_name,h.name FROM students s JOIN homework h ON h.id=%s WHERE s.id=%s',
                (sub['homework_id'], sub['student_id']),
            )
            names = cursor.fetchone()
            if not names:
                raise WorkflowError('file_not_found', 404)
            timestamp = sub['submitted_at_utc'] or file_row['created_at']
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc).astimezone(MOSCOW)
            filename = safe_pdf_filename(names['full_name'], names['name'], timestamp)
            return {
                'url': self.storage.presign_download(file_row['object_key'], filename, inline=not download),
                'filename': filename,
                'size_bytes': file_row['size_bytes'],
                'page_count': file_row['page_count'],
                'expires_in': int(self.config['S3_PRESIGN_TTL_SECONDS']),
            }

    @staticmethod
    def enqueue_delete(key):
        if not key:
            return
        with db.transaction() as (_, cursor):
            cursor.execute(
                "INSERT IGNORE INTO homework_s3_delete_queue (object_key,status,available_at) "
                "VALUES (%s,'queued',UTC_TIMESTAMP(6))",
                (key,),
            )

    def archive(self, actor, filters):
        if actor['role'] not in {'admin', 'staff_admin', 'proctor'}:
            raise WorkflowError('forbidden', 403)
        limit, after = self._page_values(filters.get('limit', 50), filters.get('after', 0))
        with db.read_cursor() as cursor:
            self._identity(cursor, actor)
            where = ["sub.state='graded'", 'sub.current_file_id IS NOT NULL']
            params = []
            if actor['role'] == 'proctor':
                where.append('EXISTS (SELECT 1 FROM proctors p WHERE p.group_id=s.group_id AND p.id=%s)')
                params.append(actor['id'])
            for key, column in (
                ('student_id', 'sub.student_id'), ('homework_id', 'sub.homework_id'), ('group_id', 's.group_id'),
            ):
                if filters.get(key):
                    try:
                        value = int(filters[key])
                    except (TypeError, ValueError) as exc:
                        raise WorkflowError('invalid_filter') from exc
                    where.append(f'{column}=%s')
                    params.append(value)
            for key, condition in [('date_from', 'sub.submitted_at_utc>=%s'),
                                   ('date_to', 'sub.submitted_at_utc<DATE_ADD(%s,INTERVAL 1 DAY)')]:
                if filters.get(key):
                    try:
                        day = dt.date.fromisoformat(filters[key])
                    except (TypeError, ValueError) as exc:
                        raise WorkflowError('invalid_filter') from exc
                    where.append(condition)
                    params.append(day)
            search = str(filters.get('search') or '').strip()[:200]
            if search:
                where.append('(s.full_name LIKE %s OR h.name LIKE %s OR g.name LIKE %s)')
                params.extend(['%' + search + '%'] * 3)
            joins = (' FROM homework_submissions sub JOIN homework_submission_files f ON f.id=sub.current_file_id '
                     'JOIN students s ON s.id=sub.student_id JOIN homework h ON h.id=sub.homework_id '
                     'LEFT JOIN `groups` g ON g.id=s.group_id '
                     'LEFT JOIN homework_sessions hs ON hs.homework_id=sub.homework_id AND hs.student_id=sub.student_id ')
            base = joins + 'WHERE ' + ' AND '.join(where)
            cursor.execute('SELECT COUNT(*) total' + base, tuple(params))
            total = int((cursor.fetchone() or {}).get('total', 0))
            cursor.execute(
                'SELECT sub.id,sub.homework_id,sub.student_id,sub.state,sub.submitted_at_utc,f.size_bytes,f.page_count,'
                's.full_name student_name,h.name homework_name,h.deadline,g.name group_name,hs.result,hs.date_pass' + base +
                (' AND sub.id<%s' if after else '') + ' ORDER BY sub.id DESC LIMIT %s',
                tuple(params + ([after] if after else []) + [limit + 1]),
            )
            rows = cursor.fetchall()
            items = self._decorate_review_items(cursor, rows[:limit])
            return {'items': items, 'next_cursor': items[-1]['id'] if items else None,
                    'total': total, 'has_more': len(rows) > limit}
