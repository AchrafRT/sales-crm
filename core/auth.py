#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Password + session logic (stdlib only)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from typing import Dict, Any, Optional

from .utils import read_json, write_json, now_iso


def _pbkdf2(password: str, salt: bytes, rounds: int = 120_000) -> bytes:
    return hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, rounds)


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = _pbkdf2(password, salt)
    return 'pbkdf2$sha256$120000$' + base64.b64encode(salt).decode() + '$' + base64.b64encode(dk).decode()


def verify_password(password: str, stored: str) -> bool:
    """Verify password against stored hash.

    Stored format: pbkdf2$sha256$120000$<salt_b64>$<dk_b64>
    """
    try:
        parts = (stored or '').split('$')
        if len(parts) != 5:
            return False
        _, algo, rounds_s, salt_b64, dk_b64 = parts
        rounds = int(rounds_s)
        salt = base64.b64decode(salt_b64)
        dk = base64.b64decode(dk_b64)
        test = hashlib.pbkdf2_hmac(algo, password.encode('utf-8'), salt, rounds)
        return hmac.compare_digest(dk, test)
    except Exception:
        return False


def new_session_id() -> str:
    return hashlib.sha256((str(time.time()) + os.urandom(16).hex()).encode()).hexdigest()[:32]


def new_session(data_dir: str, user_id: str) -> str:
    path = os.path.join(data_dir, 'sessions.json')
    sessions = read_json(path, {})
    sid = new_session_id()
    sessions[sid] = {'user_id': user_id, 'created_at': now_iso(), 'last_seen': now_iso()}
    write_json(path, sessions)
    return sid


def get_session(data_dir: str, sid: str) -> Optional[Dict[str, Any]]:
    if not sid:
        return None
    path = os.path.join(data_dir, 'sessions.json')
    sessions = read_json(path, {})
    s = sessions.get(sid)
    if s:
        s['last_seen'] = now_iso()
        sessions[sid] = s
        write_json(path, sessions)
    return s


def delete_session(data_dir: str, sid: str) -> None:
    path = os.path.join(data_dir, 'sessions.json')
    sessions = read_json(path, {})
    if sid in sessions:
        sessions.pop(sid, None)
        write_json(path, sessions)


def find_user(data_dir: str, username: str) -> Optional[Dict[str, Any]]:
    users = read_json(os.path.join(data_dir, 'users.json'), {})
    username = (username or '').strip().lower()
    for u in users.values():
        if (u.get('username') or '').lower() == username and u.get('active', True):
            return u
    return None


def get_user_by_id(data_dir: str, user_id: str) -> Optional[Dict[str, Any]]:
    users = read_json(os.path.join(data_dir, 'users.json'), {})
    return users.get(user_id)


def upsert_user(data_dir: str, user: Dict[str, Any]) -> None:
    path = os.path.join(data_dir, 'users.json')
    users = read_json(path, {})
    users[user['id']] = user
    write_json(path, users)
