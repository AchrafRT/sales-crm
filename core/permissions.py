#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, Any


def can_view_lead(user: Dict[str, Any], lead: Dict[str, Any]) -> bool:
    if not user:
        return False
    if user.get('role') == 'admin':
        return True
    if user.get('role') == 'employee':
        return lead.get('assigned_to') == user.get('id')
    if user.get('role') == 'delivery':
        return False
    return False


def can_edit_lead(user: Dict[str, Any], lead: Dict[str, Any]) -> bool:
    if user.get('role') == 'admin':
        return True
    if user.get('role') == 'employee':
        return lead.get('assigned_to') == user.get('id')
    return False


def can_view_order(user: Dict[str, Any], order: Dict[str, Any]) -> bool:
    if user.get('role') == 'admin':
        return True
    if user.get('role') == 'employee':
        return order.get('created_by') == user.get('id')
    if user.get('role') == 'delivery':
        return order.get('status') in ('scheduled','delivered')
    return False


def can_view_event(user: Dict[str, Any], event: Dict[str, Any]) -> bool:
    if user.get('role') == 'admin':
        return True
    vis = event.get('visible_to') or []
    if user.get('role') in vis:
        return True
    if user.get('id') in vis:
        return True
    return False
