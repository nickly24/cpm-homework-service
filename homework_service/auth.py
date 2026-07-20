from functools import wraps

import jwt
from flask import current_app, jsonify, request


def decode_token(token, secret):
    payload = jwt.decode(
        token,
        secret,
        algorithms=['HS256'],
        options={'require': ['exp', 'iat', 'id', 'role']},
    )
    return {
        'id': int(payload['id']),
        'role': str(payload['role']),
        'full_name': payload.get('full_name'),
        'group_id': payload.get('group_id'),
    }


def current_actor():
    header = request.headers.get('Authorization', '')
    if not header.startswith('Bearer '):
        return None
    token = header[7:].strip()
    if not token:
        return None
    try:
        return decode_token(token, current_app.config['JWT_SECRET_KEY'])
    except (jwt.InvalidTokenError, TypeError, ValueError):
        return None


def authenticated(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        actor = current_actor()
        if not actor:
            return jsonify({'ok': False, 'error': 'unauthorized'}), 401
        if actor['role'] == 'supervisor':
            return jsonify({'ok': False, 'error': 'forbidden'}), 403
        kwargs['actor'] = actor
        return view(*args, **kwargs)
    return wrapper


def roles(*allowed):
    def decorator(view):
        @wraps(view)
        @authenticated
        def wrapper(*args, **kwargs):
            actor = kwargs['actor']
            if actor['role'] not in allowed:
                return jsonify({'ok': False, 'error': 'forbidden'}), 403
            return view(*args, **kwargs)
        return wrapper
    return decorator

