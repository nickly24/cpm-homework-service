"""Live section permissions for staff_admin; uses the same tables as the main API."""
from . import db


def has_permission(actor, section, action='view'):
    if actor.get('role') == 'admin':
        return True
    flags = (actor.get('permissions') or {}).get(section) or {}
    return flags.get('edit') is True if action == 'edit' else flags.get('view') is True or flags.get('edit') is True


def load_actor(actor):
    version = actor.get('session_version')
    if type(version) is not int or version < 1:
        return None
    with db.read_cursor() as cursor:
        cursor.execute("SELECT u.id,u.full_name,u.role_id,u.is_active,u.session_version FROM admin_role_users u "
                       "JOIN auth_users a ON a.ref_id=u.id AND a.role='staff_admin' WHERE u.id=%s", (actor['id'],))
        row = cursor.fetchone()
        if not row or not row['is_active'] or row['session_version'] != version:
            return None
        cursor.execute('SELECT section,can_view,can_edit FROM admin_role_permissions WHERE role_id=%s', (row['role_id'],))
        permissions = {r['section']: {'view': bool(r['can_view']), 'edit': bool(r['can_edit'])} for r in cursor.fetchall()}
    return {**actor, 'full_name': row['full_name'], 'permissions': permissions}


RULES = {
    'workspace': (('review-queue', 'view'), ('homework-archive', 'view')),
    'review_queue': (('review-queue', 'view'),),
    'file_url': (('review-queue', 'view'), ('homework-archive', 'view')),
    'archive': (('homework-archive', 'view'),),
    'monitoring': (('monitoring', 'view'),),
    'job': (('monitoring', 'view'),),
    'retry': (('monitoring', 'edit'),),
    'transition_claim': (('review-queue', 'edit'),),
    'transition_takeover': (('review-queue', 'edit'),),
    'transition_release': (('review-queue', 'edit'),),
    'transition_request-revision': (('review-queue', 'edit'),),
    'transition_grade': (('review-queue', 'edit'),),
    'transition_edit-grade': (('homework-archive', 'edit'),),
    'transition_resubmit': (('homework-archive', 'edit'),),
}


def allowed(actor, endpoint):
    name = (endpoint or '').removeprefix('homework_api.')
    return any(has_permission(actor, section, action) for section, action in RULES.get(name, ()))
