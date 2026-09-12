from flask import Blueprint, current_app, jsonify, request

from . import db
from .auth import authenticated, roles
from .jobs import runner_status
from .workflow import OMITTED, WorkflowError


api = Blueprint('homework_api', __name__, url_prefix='/api')


def service():
    return current_app.extensions['homework_workflow']


@api.errorhandler(WorkflowError)
def workflow_error(exc):
    payload = {'ok': False, 'error': exc.code}
    if exc.details is not None:
        payload['details'] = exc.details
    return jsonify(payload), exc.status


@api.get('/workspaces/<int:homework_id>')
@authenticated
def workspace(homework_id, actor=None):
    return jsonify(service().workspace(actor, homework_id, request.args.get('student_id', type=int)))


@api.post('/workspaces/<int:homework_id>/uploads')
@roles('student')
def create_upload(homework_id, actor=None):
    body = request.get_json(silent=True) or {}
    client_id = body.get('client_upload_id') or request.headers.get('Idempotency-Key')
    if not client_id:
        raise WorkflowError('client_upload_id_required')
    return jsonify(service().create_upload(actor, homework_id, client_id)), 201


@api.post('/uploads/<job_id>/complete')
@roles('student')
def complete_upload(job_id, actor=None):
    return jsonify(service().complete_upload(actor, job_id)), 202


@api.get('/jobs/active')
@roles('student')
def active_jobs(actor=None):
    return jsonify(service().active_jobs(actor))


@api.get('/jobs/<job_id>')
@authenticated
def job(job_id, actor=None):
    return jsonify(service().job(actor, job_id))


@api.post('/jobs/<job_id>/cancel')
@roles('student')
def cancel(job_id, actor=None):
    return jsonify(service().cancel_job(actor, job_id))


@api.post('/jobs/<job_id>/retry')
@authenticated
def retry(job_id, actor=None):
    return jsonify(service().retry_job(actor, job_id)), 202


@api.post('/workspaces/<int:homework_id>/submit')
@roles('student')
def submit(homework_id, actor=None):
    return jsonify(service().submit(actor, homework_id))


@api.delete('/workspaces/<int:homework_id>/draft')
@roles('student')
def remove_draft(homework_id, actor=None):
    return jsonify(service().remove_draft(actor, homework_id))


@api.get('/review-queue')
@roles('proctor', 'admin')
def review_queue(actor=None):
    return jsonify(service().review_queue(
        actor,
        request.args.get('state'),
        request.args.get('limit', 50, type=int),
        request.args.get('after', 0, type=int),
        search=request.args.get('search'),
    ))


def register_transition(action):
    @api.post(f'/submissions/<int:submission_id>/{action}', endpoint=f'transition_{action}')
    @roles('proctor', 'admin')
    def transition(submission_id, actor=None):
        body = request.get_json(silent=True) or {}
        return jsonify(service().transition(
            actor,
            submission_id,
            action,
            message=body.get('message'),
            result=body.get('result', OMITTED),
        ))


for transition_name in ('claim', 'takeover', 'release', 'request-revision', 'grade', 'edit-grade', 'resubmit'):
    register_transition(transition_name)


@api.get('/submissions/<int:submission_id>/file-url')
@authenticated
def file_url(submission_id, actor=None):
    return jsonify(service().file_url(
        actor,
        submission_id,
        draft=request.args.get('draft') == '1',
        download=request.args.get('download') == '1',
    ))


@api.get('/archive')
@roles('admin', 'proctor')
def archive(actor=None):
    return jsonify(service().archive(actor, request.args))


@api.get('/monitoring')
@roles('admin')
def monitoring(actor=None):
    with db.read_cursor() as cursor:
        service()._identity(cursor, actor)
        cursor.execute('SELECT status,COUNT(*) count FROM homework_file_jobs GROUP BY status')
        totals = cursor.fetchall()
        cursor.execute(
            'SELECT id,status,stage,progress,error_code,attempts,manual_attempts,created_at '
            'FROM homework_file_jobs ORDER BY created_at DESC LIMIT 50'
        )
        recent = cursor.fetchall()
        cursor.execute(
            "SELECT COUNT(*) count FROM homework_file_jobs WHERE status='failed' "
            'AND updated_at>=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 HOUR)'
        )
        failed_recent = cursor.fetchone()['count']
    runner = runner_status()
    warnings = []
    if not runner['started'] or (runner['heartbeat_age_seconds'] or 0) > 10:
        warnings.append('runner_unavailable')
    if failed_recent >= 5:
        warnings.append('failed_jobs_growth')
    try:
        storage = current_app.extensions['homework_storage'].size_summary()
    except Exception:
        storage = {'file_count': None, 'total_bytes': None}
        warnings.append('storage_unavailable')
    return jsonify({
        'jobs': totals,
        'recent_jobs': recent,
        'failed_last_hour': failed_recent,
        'runner': runner,
        'storage': storage,
        'warnings': warnings,
    })

