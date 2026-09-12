import os


def _integer(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return int(default)


def _boolean(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {'1', 'true', 'yes', 'on'}


class Config:
    ENV = os.getenv('ENV', 'production')
    PORT = _integer('PORT', 8001)
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', '')

    MYSQL_HOST = os.getenv('MYSQL_HOST', '127.0.0.1')
    MYSQL_PORT = _integer('MYSQL_PORT', 3306)
    MYSQL_USER = os.getenv('MYSQL_USER', '')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', '')
    MYSQL_POOL_SIZE = _integer('MYSQL_POOL_SIZE', 8)

    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'https://s3.twcstorage.ru')
    S3_REGION = os.getenv('S3_REGION', 'ru-1')
    S3_BUCKET = os.getenv('S3_BUCKET', '')
    S3_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID', '')
    S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY', '')
    S3_PRESIGN_TTL_SECONDS = _integer('S3_PRESIGN_TTL_SECONDS', 300)

    PDF_MAX_BYTES = _integer('PDF_MAX_BYTES', 10 * 1024 * 1024)
    PDF_MAX_PAGES = _integer('PDF_MAX_PAGES', 35)
    JOB_STALE_SECONDS = _integer('JOB_STALE_SECONDS', 180)
    UPLOAD_STALE_SECONDS = _integer('UPLOAD_STALE_SECONDS', 1800)
    JOB_POLL_SECONDS = _integer('JOB_POLL_SECONDS', 2)
    RUN_HOMEWORK_WORKER = _boolean('RUN_HOMEWORK_WORKER', True)
    CORS_ORIGINS_EXTRA = os.getenv('CORS_ORIGINS_EXTRA', '')

    pass


def validate_config(config):
    required = (
        'JWT_SECRET_KEY', 'MYSQL_USER', 'MYSQL_DATABASE', 'S3_BUCKET',
        'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY',
    )
    missing = [name for name in required if not config.get(name)]
    if missing:
        raise RuntimeError('missing_configuration:' + ','.join(missing))


def cors_origins(config):
    origins = {
        'https://cpm-lms.ru',
        'http://localhost:3000',
        'http://127.0.0.1:3000',
    }
    extra = config.get('CORS_ORIGINS_EXTRA', '')
    origins.update(item.strip() for item in extra.split(',') if item.strip())
    return sorted(origins)
