#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Write commands to inbox folder."""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, Dict

from .utils import ensure_dir, now_iso


def write_command(data_dir: str, cmd: str, actor: str, payload: Dict[str, Any]) -> str:
    inbox = os.path.join(data_dir, 'inbox')
    ensure_dir(inbox)
    ts = time.strftime('%Y%m%dT%H%M%S', time.localtime())
    cid = uuid.uuid4().hex[:8].upper()
    filename = f'CMD_{ts}_{cid}.json'
    path = os.path.join(inbox, filename)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({
            'cmd': cmd,
            'actor': actor,
            'created_at': now_iso(),
            'payload': payload or {},
        }, f, ensure_ascii=False, indent=2)
    return path
