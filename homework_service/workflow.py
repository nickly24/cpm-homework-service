import datetime as dt
import uuid
from zoneinfo import ZoneInfo

from . import db
from .storage import safe_pdf_filename


MOSCOW = ZoneInfo('Europe/Moscow')
ACTIVE_JOB_STATUSES = ('uploading', 'queued', 'running', 'retry')


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
    def _identity(cursor, actor):
        tables = {'student': 'students', 'proctor': 'proctors', 'admin': 'admins'}
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
        if actor['role'] == 'admin':
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
            cursor.execute(
                'SELECT id,status,result,date_pass FROM homework_sessions '
                'WHERE homework_id=%s AND student_id=%s',
                (homework_id, student_id),
            )
            legacy = cursor.fetchone()
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
                            {'role': submission['reviewer_role'], 'id': submission['reviewer_id']}
                            if submission['reviewer_id'] else None
                        ),
                    }
            state = submission['state'] if submission else 'none'
            graded = bool(legacy and legacy['status'])
            return {
                'homework': homework,
                'legacy_result': legacy,
                'submission': visible or {'state': 'none', 'has_file': False, 'has_draft': False},
                'permissions': {
                    'upload': actor['role'] == 'student' and not graded and state in {
                        'none', 'uploading', 'processing', 'draft', 'revision_requested',
                    },
                    'submit': actor['role'] == 'student' and bool(
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
            cursor.execute(
                'SELECT status FROM homework_sessions WHERE homework_id=%s AND student_id=%s',
                (homework_id, actor['id']),
            )
            legacy = cursor.fetchone()
            if legacy and legacy['status']:
                raise WorkflowError('already_graded', 409)
            if submission['state'] not in {
                'none', 'uploading', 'processing', 'draft', 'revision_requested',
            }:
                raise WorkflowError('file_locked_after_submit', 409)
            cursor.execute(
                'SELECT * FROM homework_file_jobs WHERE student_id=%s AND client_upload_id=%s',
                (actor['id'], client_upload_id),
            )
            job = cursor.fetchone()
            if not job:
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
            cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s FOR UPDATE', (job_id,))
            job = cursor.fetchone()
            if not job:
                raise WorkflowError('job_not_found', 404)
            self._access_student(cursor, actor, job['student_id'])
            if actor['role'] != 'student':
                raise WorkflowError('forbidden', 403)
            if job['status'] != 'uploading':
                return _job_json(job)
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
            cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s FOR UPDATE', (job_id,))
            job = cursor.fetchone()
            if not job:
                raise WorkflowError('job_not_found', 404)
            self._access_student(cursor, actor, job['student_id'])
            if actor['role'] != 'student':
                raise WorkflowError('forbidden', 403)
            if job['status'] == 'cancelled':
                return _job_json(job)
            if job['status'] in {'ready', 'failed'}:
                raise WorkflowError('job_not_cancellable', 409)
            key = job['staging_key']
            cursor.execute(
                "UPDATE homework_file_jobs SET status='cancelled',stage='cancelled',"
                'lease_expires_at=UTC_TIMESTAMP(6),updated_at=UTC_TIMESTAMP(6) WHERE id=%s',
                (job_id,),
            )
            cursor.execute(
                "UPDATE homework_submissions SET state=CASE "
                "WHEN current_file_id IS NOT NULL THEN state "
                "WHEN draft_file_id IS NOT NULL THEN 'draft' ELSE 'none' END WHERE id=%s",
                (job['submission_id'],),
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
            cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s FOR UPDATE', (job_id,))
            job = cursor.fetchone()
            if not job:
                raise WorkflowError('job_not_found', 404)
            self._access_student(cursor, actor, job['student_id'])
            if job['status'] != 'failed':
                if job['status'] in {'queued', 'retry', 'running', 'ready'} and int(job['manual_attempts']) > 0:
                    return _job_json(job)
                raise WorkflowError('job_not_retryable', 409)
            if int(job['manual_attempts']) >= 3:
                raise WorkflowError('manual_retry_limit', 409)
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
            cursor.execute('SELECT * FROM homework_file_jobs WHERE id=%s', (job_id,))
            return _job_json(cursor.fetchone())

    def submit(self, actor, homework_id):
        if actor['role'] != 'student':
            raise WorkflowError('forbidden', 403)
        with db.transaction() as (_, cursor):
            self._identity(cursor, actor)
            self._homework(cursor, homework_id, published=True)
            sub = self._submission(cursor, homework_id, actor['id'], lock=True)
            if not sub or not sub['draft_file_id']:
                raise WorkflowError('draft_not_ready', 409)
            if sub['state'] not in {'draft', 'revision_requested'}:
                if sub['state'] in {'submitted', 'in_review'}:
                    return {'state': sub['state'], 'submitted_at_utc': _iso(sub['submitted_at_utc'])}
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

    def review_queue(self, actor, state=None, limit=50, after=0):
        if actor['role'] not in {'proctor', 'admin'}:
            raise WorkflowError('forbidden', 403)
        with db.transaction() as (_, cursor):
            self._identity(cursor, actor)
            cursor.execute(
                "UPDATE homework_submissions sub JOIN students s ON s.id=sub.student_id "
                "LEFT JOIN proctors current_p ON current_p.group_id=s.group_id "
                "SET sub.state='submitted',sub.reviewer_role=NULL,sub.reviewer_id=NULL "
                "WHERE sub.state='in_review' AND sub.reviewer_role='proctor' "
                "AND (current_p.id IS NULL OR current_p.id<>sub.reviewer_id)"
            )
            states = [state] if state else ['submitted', 'in_review', 'revision_requested']
            if any(value not in {'submitted', 'in_review', 'revision_requested'} for value in states):
                raise WorkflowError('invalid_state')
            params = [int(after)]
            scope = ''
            if actor['role'] == 'proctor':
                scope = ' AND p.id=%s'
                params.append(actor['id'])
            marks = ','.join(['%s'] * len(states))
            params.extend(states)
            params.append(min(max(int(limit), 1), 100))
            cursor.execute(
                'SELECT sub.id,sub.homework_id,sub.student_id,sub.state,sub.submitted_at_utc,'
                'sub.reviewer_role,sub.reviewer_id,sub.revision_comment,h.name homework_name,h.deadline,'
                's.full_name student_name,g.name group_name FROM homework_submissions sub '
                'JOIN homework h ON h.id=sub.homework_id JOIN students s ON s.id=sub.student_id '
                'LEFT JOIN `groups` g ON g.id=s.group_id LEFT JOIN proctors p ON p.group_id=s.group_id '
                f'WHERE sub.id>%s{scope} AND sub.state IN ({marks}) ORDER BY sub.id LIMIT %s',
                tuple(params),
            )
            items = cursor.fetchall()
            for item in items:
                item['submitted_at_utc'] = _iso(item['submitted_at_utc'])
            return {'items': items, 'next_cursor': items[-1]['id'] if items else None}

    def transition(self, actor, submission_id, action, message=None, result=None):
        if actor['role'] not in {'proctor', 'admin'}:
            raise WorkflowError('forbidden', 403)
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
                if actor['role'] != 'admin' or sub['state'] != 'in_review':
                    raise WorkflowError('invalid_state', 409)
                cursor.execute(
                    "UPDATE homework_submissions SET reviewer_role='admin',reviewer_id=%s WHERE id=%s",
                    (actor['id'], submission_id),
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
                    if existing and (result is None or int(existing['result']) == self._score(result)):
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
                if sub['state'] == 'none' and not sub['current_file_id'] and not sub['draft_file_id']:
                    return {'ok': True, 'result': None}
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
        if actor['role'] != 'admin' and int(sub['reviewer_id'] or 0) != int(actor['id']):
            raise WorkflowError('not_reviewer', 409)

    @staticmethod
    def _score(value):
        try:
            score = int(value)
        except (TypeError, ValueError) as exc:
            raise WorkflowError('invalid_result') from exc
        if not 0 <= score <= 100:
            raise WorkflowError('invalid_result')
        return score

    def _grade(self, cursor, sub, requested):
        cursor.execute('SELECT deadline FROM homework WHERE id=%s', (sub['homework_id'],))
        deadline = cursor.fetchone()['deadline']
        submitted_at = sub['submitted_at_utc']
        if not submitted_at:
            raise WorkflowError('submission_timestamp_missing', 409)
        submitted_date = submitted_at.replace(tzinfo=dt.timezone.utc).astimezone(MOSCOW).date()
        suggested = 100 if deadline is None else max(0, 100 - 5 * max(0, (submitted_date - deadline).days))
        score = suggested if requested is None else self._score(requested)
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
            file_id = sub['draft_file_id'] if draft else sub['current_file_id']
            if not file_id:
                raise WorkflowError('file_not_found', 404)
            cursor.execute('SELECT object_key FROM homework_submission_files WHERE id=%s', (file_id,))
            file_row = cursor.fetchone()
            cursor.execute(
                'SELECT s.full_name,h.name FROM students s JOIN homework h ON h.id=%s WHERE s.id=%s',
                (sub['homework_id'], sub['student_id']),
            )
            names = cursor.fetchone()
            timestamp = sub['submitted_at_utc'] or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc).astimezone(MOSCOW)
            filename = safe_pdf_filename(names['full_name'], names['name'], timestamp)
            return {
                'url': self.storage.presign_download(file_row['object_key'], filename, inline=not download),
                'filename': filename,
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
        if actor['role'] != 'admin':
            raise WorkflowError('forbidden', 403)
        with db.read_cursor() as cursor:
            self._identity(cursor, actor)
            where = ["sub.state='graded'", 'sub.current_file_id IS NOT NULL']
            params = []
            for key, column in (
                ('student_id', 'sub.student_id'), ('homework_id', 'sub.homework_id'), ('group_id', 's.group_id'),
            ):
                if filters.get(key):
                    where.append(f'{column}=%s')
                    params.append(int(filters[key]))
            if filters.get('date_from'):
                where.append('sub.submitted_at_utc>=%s')
                params.append(filters['date_from'])
            if filters.get('date_to'):
                where.append('sub.submitted_at_utc<DATE_ADD(%s,INTERVAL 1 DAY)')
                params.append(filters['date_to'])
            cursor.execute(
                'SELECT sub.id,sub.homework_id,sub.student_id,sub.submitted_at_utc,f.size_bytes,f.page_count,'
                's.full_name student_name,h.name homework_name,g.name group_name FROM homework_submissions sub '
                'JOIN homework_submission_files f ON f.id=sub.current_file_id '
                'JOIN students s ON s.id=sub.student_id JOIN homework h ON h.id=sub.homework_id '
                'LEFT JOIN `groups` g ON g.id=s.group_id WHERE ' + ' AND '.join(where) +
                ' ORDER BY sub.submitted_at_utc DESC LIMIT 200',
                tuple(params),
            )
            items = cursor.fetchall()
            for item in items:
                item['submitted_at_utc'] = _iso(item['submitted_at_utc'])
            return {'items': items}
