import logging
import uuid

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

from . import db
from .api import api
from .config import Config, cors_origins, validate_config
from .jobs import start_runner
from .storage import HomeworkStorage
from .workflow import HomeworkWorkflow


def create_app(overrides=None, initialize_services=True, run_worker=None):
    app = Flask(__name__)
    app.config.from_object(Config)
    if overrides:
        app.config.update(overrides)
    CORS(
        app,
        origins=cors_origins(app.config),
        allow_headers=['Authorization', 'Content-Type', 'Idempotency-Key'],
        methods=['GET', 'POST', 'OPTIONS'],
        supports_credentials=False,
        max_age=600,
    )
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('cpm_homework_service')

    @app.get('/health')
    def health():
        return jsonify({'ok': True, 'service': 'cpm-homework-service'})

    @app.before_request
    def reject_large_json():
        request.correlation_id = request.headers.get('X-Correlation-ID') or str(uuid.uuid4())
        if request.content_length and request.content_length > 1024 * 1024:
            return jsonify({'ok': False, 'error': 'request_too_large'}), 413

    @app.after_request
    def correlation_header(response):
        response.headers['X-Correlation-ID'] = getattr(request, 'correlation_id', str(uuid.uuid4()))
        return response

    @app.errorhandler(Exception)
    def unexpected_error(exc):
        if isinstance(exc, HTTPException):
            return jsonify({'ok': False, 'error': exc.name.lower().replace(' ', '_')}), exc.code
        logger.error(
            'request_failed correlation_id=%s error_code=%s',
            getattr(request, 'correlation_id', 'missing'),
            type(exc).__name__.lower(),
        )
        return jsonify({'ok': False, 'error': 'internal_error'}), 500

    if initialize_services:
        validate_config(app.config)
        db.init_pool(app.config)
        storage = HomeworkStorage(app.config)
        app.extensions['homework_storage'] = storage
        app.extensions['homework_workflow'] = HomeworkWorkflow(app.config, storage)
    app.register_blueprint(api)
    should_run_worker = app.config.get('RUN_HOMEWORK_WORKER', True) if run_worker is None else run_worker
    if initialize_services and should_run_worker:
        start_runner(app)
    return app
