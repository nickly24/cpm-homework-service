from contextlib import contextmanager

from mysql.connector import pooling


_pool = None


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


@contextmanager
def transaction(dictionary=True):
    conn = connection()
    cursor = conn.cursor(dictionary=dictionary)
    try:
        yield conn, cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


@contextmanager
def read_cursor(dictionary=True):
    conn = connection()
    cursor = conn.cursor(dictionary=dictionary)
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()

