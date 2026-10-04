from contextlib import contextmanager
from contextvars import ContextVar

from mysql.connector import Error as MySQLError, pooling


_pool = None
_protection_installed = False
_fence_depth = ContextVar('student_write_fence_depth', default=0)
_fence_connection = ContextVar('student_write_fence_connection', default=None)
FENCE_NAME = 'cpm:student-write-fence'


def init_pool(config):
    global _pool
    _pool = pooling.MySQLConnectionPool(
        pool_name='cpm_homework_service_pool',
        pool_size=int(config['MYSQL_POOL_SIZE']),
        pool_reset_session=True,
        host=config['MYSQL_HOST'],
        port=int(config['MYSQL_PORT']),
        user=config['MYSQL_USER'],
        password=config['MYSQL_PASSWORD'],
        database=config['MYSQL_DATABASE'],
        autocommit=False,
    )
    return _pool


def connection():
    if _pool is None:
        raise RuntimeError('mysql_pool_not_initialized')
    return _pool.get_connection()


def deletion_enabled():
    from flask import current_app, has_app_context
    if has_app_context():
        return bool(current_app.config.get('STUDENT_DELETION_ENABLED', False))
    from .config import Config
    return bool(Config.STUDENT_DELETION_ENABLED)


def deletion_protection_active():
    """Installed tombstones remain authoritative when new deletion is disabled."""
    global _protection_installed
    if deletion_enabled() or _protection_installed:
        return True
    if _pool is None:
        return False  # Dependency-free app tests have no initialized DB.
    try:
        with read_cursor() as cursor:
            cursor.execute('SELECT 1 FROM student_deletion_barriers LIMIT 0')
            cursor.fetchall()
    except MySQLError as exc:
        if exc.errno == 1146:  # Only an actually absent table permits legacy mode.
            return False
        raise  # Permission failures or outages must never reopen a tombstone.
    _protection_installed = True
    return True


@contextmanager
def student_write_fence(conn=None):
    """Cross-service lock. Nested transactions inherit the current thread's fence."""
    if not deletion_protection_active():
        yield
        return
    depth = _fence_depth.get()
    if depth:
        token = _fence_depth.set(depth + 1)
        try:
            yield
        finally:
            _fence_depth.reset(token)
        return
    owned = conn is None
    conn = conn or connection()
    cursor = conn.cursor(dictionary=True)
    held = False
    token = connection_token = None
    try:
        cursor.execute('SELECT GET_LOCK(%s,30) acquired', (FENCE_NAME,))
        held = (cursor.fetchone() or {}).get('acquired') == 1
        if not held:
            raise RuntimeError('student_write_fence_unavailable')
        token = _fence_depth.set(1)
        connection_token = _fence_connection.set(conn)
        yield
    finally:
        if connection_token is not None:
            _fence_connection.reset(connection_token)
        if token is not None:
            _fence_depth.reset(token)
        try:
            if held:
                cursor.execute('SELECT RELEASE_LOCK(%s) released', (FENCE_NAME,))
                cursor.fetchone()
        finally:
            cursor.close()
            if owned:
                conn.close()


@contextmanager
def transaction(dictionary=True):
    outer = _fence_connection.get()
    conn = outer or connection()
    cursor = conn.cursor(dictionary=dictionary)
    try:
        with student_write_fence(conn):
            try:
                yield conn, cursor
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    finally:
        cursor.close()
        if outer is None:
            conn.close()


def student_is_blocked(cursor, student_id):
    if not deletion_protection_active():
        return False
    # A current read prevents a REPEATABLE READ snapshot preceding the barrier.
    cursor.execute('SELECT student_id FROM student_deletion_barriers WHERE student_id=%s FOR UPDATE',
                   (student_id,))
    return cursor.fetchone() is not None


def require_student_writable(cursor, student_id):
    if not deletion_protection_active():
        return
    from .workflow import WorkflowError
    if student_is_blocked(cursor, student_id):
        raise WorkflowError('student_deletion_in_progress', 409)
    cursor.execute("SELECT s.id FROM students s JOIN auth_users a ON a.ref_id=s.id AND a.role='student' WHERE s.id=%s FOR UPDATE", (student_id,))
    if not cursor.fetchone():
        raise WorkflowError('account_not_found', 401)


@contextmanager
def read_cursor(dictionary=True):
    conn = connection()
    cursor = conn.cursor(dictionary=dictionary)
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()

