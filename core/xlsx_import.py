#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from typing import List, Dict, Any, Tuple


def parse_leads_file(path: str) -> Tuple[List[Dict[str, Any]], str]:
    """Parse leads from .xlsx (openpyxl if available) or .csv.

    Expected columns (case-insensitive, flexible):
      - business name
      - business phone
      - business address

    Returns (rows, message).
    """
    ext = os.path.splitext(path)[1].lower()

    if ext == '.csv':
        import csv
        rows: List[Dict[str, Any]] = []
        with open(path, 'r', encoding='utf-8', errors='ignore', newline='') as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append({k.strip(): (v or '').strip() for k, v in r.items()})
        return rows, 'csv'

    if ext in ('.xlsx', '.xlsm', '.xltx', '.xltm'):
        try:
            import openpyxl
        except Exception:
            return [], 'openpyxl not available; upload csv instead'
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        header = []
        rows_out: List[Dict[str, Any]] = []
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                header = [str(c).strip() if c is not None else '' for c in row]
                continue
            d = {}
            for j, c in enumerate(row):
                key = header[j] if j < len(header) else f'col{j+1}'
                d[key] = '' if c is None else str(c).strip()
            # skip empty rows
            if any(v for v in d.values()):
                rows_out.append(d)
        return rows_out, 'xlsx'

    return [], 'unsupported file type'


def map_lead_fields(raw: Dict[str, Any]) -> Dict[str, str]:
    # normalize keys
    def g(*keys: str) -> str:
        for k in keys:
            for rk, rv in raw.items():
                if (rk or '').strip().lower() == k:
                    return (rv or '').strip()
        return ''

    name = g('business name', 'business_name', 'name', 'company', 'company name')
    phone = g('business phone', 'business_phone', 'phone', 'telephone', 'tel')
    address = g('business address', 'business_address', 'address', 'location')
    # if not found by exact, try fuzzy contains
    if not (name or phone or address):
        for rk, rv in raw.items():
            lk = (rk or '').lower()
            if not name and 'name' in lk:
                name = (rv or '').strip()
            if not phone and ('phone' in lk or 'tel' in lk):
                phone = (rv or '').strip()
            if not address and 'address' in lk:
                address = (rv or '').strip()

    return {
        'business_name': name,
        'business_phone': phone,
        'business_address': address,
    }
