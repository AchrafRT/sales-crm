#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Deterministic helpers (stdlib only)."""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, Optional


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def now_iso() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime())


def safe_join(base: str, *parts: str) -> str:
    base = os.path.abspath(base)
    path = os.path.abspath(os.path.join(base, *parts))
    if not path.startswith(base + os.sep) and path != base:
        raise ValueError('unsafe path')
    return path


def read_json(path: str, default: Any) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        return default


def write_json(path: str, data: Any) -> None:
    # atomic-ish write
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def slug(s: str) -> str:
    s = (s or '').strip().lower()
    s = re.sub(r'[^a-z0-9]+', '-', s)
    return s.strip('-') or 'item'


def clamp_int(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(n)))


def render_template(html_text: str, ctx: Dict[str, Any]) -> str:
    # dead-simple {{key}} replacement
    out = html_text
    for k, v in ctx.items():
        out = out.replace('{{' + k + '}}', str(v))
    return out
