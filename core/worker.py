#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Command processor (deterministic)."""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Tuple

from .utils import read_json, write_json, ensure_dir, now_iso
from .auth import hash_password
from .pdf import generate_invoice, generate_order


def paths(base_dir: str) -> Dict[str, str]:
    data = os.path.join(base_dir, 'data')
    return {
        'data': data,
        'inbox': os.path.join(data, 'inbox'),
        'processed': os.path.join(data, 'processed'),
        'logs': os.path.join(data, 'logs'),
        'docs_invoices': os.path.join(data, 'docs', 'invoices'),
        'docs_orders': os.path.join(data, 'docs', 'orders'),
    }


def _log(data_dir: str, msg: str) -> None:
    ensure_dir(os.path.join(data_dir, 'logs'))
    p = os.path.join(data_dir, 'logs', 'worker.log')
    line = f"[{now_iso()}] {msg}\n"
    with open(p, 'a', encoding='utf-8') as f:
        f.write(line)


def _next_id(prefix: str, store: Dict[str, Any]) -> str:
    # stable incremental id based on existing keys
    n = 0
    for k in store.keys():
        if k.startswith(prefix):
            try:
                n = max(n, int(k[len(prefix):]))
            except Exception:
                pass
    return f"{prefix}{n+1:04d}"

def _money_fmt(amount: float, currency: str) -> str:
    try:
        return f"{amount:.2f} {currency}"
    except Exception:
        return str(amount)


def _recalc_order(order: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    """Compute totals from cases using deterministic pricing + Quebec taxes.

    Rules:
      - 24 cans per case
      - price_per_case default from settings (59.76)
      - Only quantities (cases) are editable in UI.
      - GST + QST applied on subtotal.
    """
    currency = (settings.get('currency') or 'CAD').strip() or 'CAD'
    try:
        price_per_case = float(settings.get('price_per_case') or 59.76)
    except Exception:
        price_per_case = 59.76
    cans_per_case = 24
    price_per_can = round(price_per_case / cans_per_case, 4)

    try:
        gst_rate = float(settings.get('gst_rate') if settings.get('gst_rate') is not None else 0.05)
    except Exception:
        gst_rate = 0.05
    try:
        qst_rate = float(settings.get('qst_rate') if settings.get('qst_rate') is not None else 0.09975)
    except Exception:
        qst_rate = 0.09975

    items = order.get('items') or []
    subtotal = 0.0
    for it in items:
        cases = int(it.get('cases') or 0)
        it['cans_per_case'] = cans_per_case
        it['price_per_can'] = price_per_can
        it['price_per_case'] = price_per_case
        it['line_total'] = round(price_per_case * cases, 2)
        subtotal += it['line_total']

    subtotal = round(subtotal, 2)
    gst = round(subtotal * gst_rate, 2)
    qst = round(subtotal * qst_rate, 2)
    total = round(subtotal + gst + qst, 2)

    order['pricing'] = {
        'currency': currency,
        'price_per_case': price_per_case,
        'price_per_can': price_per_can,
        'cans_per_case': cans_per_case,
        'gst_rate': gst_rate,
        'qst_rate': qst_rate,
    }
    order['totals'] = {
        'subtotal': subtotal,
        'gst': gst,
        'qst': qst,
        'total': total,
    }
    order['items'] = items
    order['total_amount'] = total
    order['total'] = _money_fmt(total, currency)
    return order



def _append_history(obj: Dict[str, Any], actor: str, action: str, detail: str = '') -> None:
    obj.setdefault('history', [])
    obj['history'].append({
        'at': now_iso(),
        'actor': actor,
        'action': action,
        'detail': detail,
    })


def process_command_file(base_dir: str, cmd_path: str) -> Tuple[bool, str]:
    """Returns (ok, message). Moves file to processed."""
    p = paths(base_dir)
    ensure_dir(p['processed'])

    try:
        with open(cmd_path, 'r', encoding='utf-8') as f:
            cmd_obj = json.load(f)
    except Exception as e:
        _log(p['data'], f"BAD_CMD {os.path.basename(cmd_path)} {e}")
        return False, 'bad command file'

    ok, msg = execute(base_dir, cmd_obj)

    # move regardless (deterministic)
    dst = os.path.join(p['processed'], os.path.basename(cmd_path))
    try:
        os.replace(cmd_path, dst)
    except Exception:
        pass

    _log(p['data'], f"{cmd_obj.get('cmd')} ok={ok} msg={msg}")
    return ok, msg


def execute(base_dir: str, cmd_obj: Dict[str, Any]) -> Tuple[bool, str]:
    data_dir = os.path.join(base_dir, 'data')
    cmd = cmd_obj.get('cmd')
    actor = cmd_obj.get('actor')
    payload = cmd_obj.get('payload') or {}

    users_path = os.path.join(data_dir, 'users.json')
    leads_path = os.path.join(data_dir, 'leads.json')
    clients_path = os.path.join(data_dir, 'clients.json')
    orders_path = os.path.join(data_dir, 'orders.json')
    invoices_path = os.path.join(data_dir, 'invoices.json')
    calendar_path = os.path.join(data_dir, 'calendar.json')
    notif_path = os.path.join(data_dir, 'notifications.json')
    settings_path = os.path.join(data_dir, 'settings.json')
    settings = read_json(settings_path, {})
    users = read_json(users_path, {})
    leads = read_json(leads_path, {})
    clients = read_json(clients_path, {})
    orders = read_json(orders_path, {})
    invoices = read_json(invoices_path, {})
    calendar = read_json(calendar_path, {})
    notifs = read_json(notif_path, {})

    # ------------------
    # Settings
    # ------------------
    if cmd == 'update_settings':
        # admin-only enforced by server
        s = read_json(settings_path, {})
        s['company_name'] = (payload.get('company_name') or s.get('company_name','')).strip()
        s['company_email'] = (payload.get('company_email') or s.get('company_email','')).strip()
        s['currency'] = (payload.get('currency') or s.get('currency','CAD')).strip() or 'CAD'
        try:
            s['price_per_case'] = float(payload.get('price_per_case') or s.get('price_per_case',59.76))
        except Exception:
            s['price_per_case'] = float(s.get('price_per_case',59.76))
        try:
            s['gst_rate'] = float(payload.get('gst_rate') or s.get('gst_rate',0.05))
        except Exception:
            s['gst_rate'] = float(s.get('gst_rate',0.05))
        try:
            s['qst_rate'] = float(payload.get('qst_rate') or s.get('qst_rate',0.09975))
        except Exception:
            s['qst_rate'] = float(s.get('qst_rate',0.09975))
        write_json(settings_path, s)
        return True, 'settings updated'

    def add_notif(ntype: str, text: str, for_role: str = 'admin', open_url: str = '') -> None:
        nid = _next_id('N', notifs)
        notifs[nid] = {'id': nid, 'type': ntype, 'text': text, 'created_at': now_iso(), 'for_role': for_role, 'read': False, 'open_url': open_url}

    # ------------------
    # Users
    # ------------------
    if cmd == 'create_employee':
        uid = _next_id('U', users)
        username = (payload.get('username') or '').strip().lower()
        password = (payload.get('password') or '').strip()
        role = payload.get('role') or 'employee'
        if role not in ('employee', 'delivery'):
            role = 'employee'
        if not username or not password:
            return False, 'missing username/password'
        for u in users.values():
            if (u.get('username') or '').lower() == username:
                return False, 'username exists'
        users[uid] = {
            'id': uid,
            'role': role,
            'username': username,
            'pass_hash': hash_password(password),
            'active': True,
            'needs_first_login': True,
            'created_at': now_iso(),
        }
        write_json(users_path, users)
        return True, 'employee created'

    if cmd == 'disable_user':
        uid = payload.get('user_id')
        if uid in users:
            users[uid]['active'] = False
            write_json(users_path, users)
            return True, 'user disabled'
        return False, 'user not found'

    if cmd == 'reset_password':
        uid = payload.get('user_id')
        pw = (payload.get('password') or '').strip()
        if uid in users and pw:
            users[uid]['pass_hash'] = hash_password(pw)
            users[uid]['needs_first_login'] = True
            write_json(users_path, users)
            return True, 'password reset'
        return False, 'bad request'

    # ------------------
    # Leads
    # ------------------
    if cmd == 'import_leads_batch':
        # payload: rows: [{business_name, business_phone, business_address}]
        rows = payload.get('rows') or []
        added = 0
        for r in rows:
            lid = _next_id('L', leads)
            leads[lid] = {
                'id': lid,
                'created_at': now_iso(),
                'status': 'new',
                'business_name': (r.get('business_name') or '').strip(),
                'business_phone': (r.get('business_phone') or '').strip(),
                'business_address': (r.get('business_address') or '').strip(),
                'assigned_to': '',
                'rep_name': '',
                'rep_phone': '',
                'rep_email': '',
                'rep_address': '',
                'notes': '',
                'last_touch_at': '',
                'history': [],
            }
            _append_history(leads[lid], actor, 'import', '')
            added += 1
        write_json(leads_path, leads)
        if added:
            add_notif('new_leads', f'{added} new leads imported', 'admin', open_url='/admin?tab=leads')
            write_json(notif_path, notifs)
        return True, f'imported {added}'

    

    if cmd == 'create_lead':
        # payload: business_name, business_phone, business_address, assigned_to(optional)
        lid = _next_id('L', leads)
        assigned_to = (payload.get('assigned_to') or '').strip()
        leads[lid] = {
            'id': lid,
            'created_at': now_iso(),
            'status': 'new',
            'business_name': (payload.get('business_name') or '').strip(),
            'business_phone': (payload.get('business_phone') or '').strip(),
            'business_address': (payload.get('business_address') or '').strip(),
            'assigned_to': assigned_to,
            'rep_name': '',
            'rep_phone': '',
            'rep_email': '',
            'rep_address': '',
            'notes': '',
            'last_touch_at': '',
            'history': [],
        }
        _append_history(leads[lid], actor, 'create', '')
        write_json(leads_path, leads)
        add_notif('new_lead', f"Lead {lid} created", 'admin', open_url=f"/lead?id={lid}")
        write_json(notif_path, notifs)
        return True, 'lead created'

    if cmd == 'delete_leads':
        # admin-only should be enforced at server, worker is deterministic executor.
        ids = payload.get('lead_ids') or []
        deleted = 0
        # cascade delete: orders, invoices, calendar events linked
        inv_deleted = 0
        ord_deleted = 0
        ev_deleted = 0
        for lid in list(ids):
            if lid in leads:
                leads.pop(lid, None)
                deleted += 1
                for oid, o in list(orders.items()):
                    if o.get('lead_id') == lid:
                        orders.pop(oid, None); ord_deleted += 1
                        for iid, iv in list(invoices.items()):
                            if iv.get('order_id') == oid:
                                invoices.pop(iid, None); inv_deleted += 1
                        for eid, ev in list(calendar.items()):
                            rel = ev.get('related') or {}
                            if rel.get('order_id') == oid or rel.get('lead_id') == lid:
                                calendar.pop(eid, None); ev_deleted += 1
        write_json(leads_path, leads)
        write_json(orders_path, orders)
        write_json(invoices_path, invoices)
        write_json(calendar_path, calendar)
        return True, f"deleted {deleted} leads ({ord_deleted} orders, {inv_deleted} invoices, {ev_deleted} events)"
    if cmd == 'assign_lead':
        lid = payload.get('lead_id')
        uid = payload.get('user_id')
        if lid in leads and uid in users:
            leads[lid]['assigned_to'] = uid
            _append_history(leads[lid], actor, 'assign', f"to {uid}")
            write_json(leads_path, leads)
            return True, 'assigned'
        return False, 'bad lead/user'

    if cmd == 'assign_leads_bulk':
        ids = payload.get('lead_ids') or []
        uid = payload.get('user_id')
        if uid not in users:
            return False, 'bad user'
        n = 0
        for lid in ids:
            if lid in leads:
                leads[lid]['assigned_to'] = uid
                _append_history(leads[lid], actor, 'assign', f"to {uid}")
                n += 1
        write_json(leads_path, leads)
        return True, f'assigned {n}'

    if cmd == 'update_lead_fields':
        lid = payload.get('lead_id')
        fields = payload.get('fields') or {}
        if lid not in leads:
            return False, 'lead not found'
        for k in ('rep_name','rep_phone','rep_email','rep_address','notes','status','last_touch_at'):
            if k in fields:
                leads[lid][k] = (fields.get(k) or '').strip()
        _append_history(leads[lid], actor, 'update', ','.join(sorted(fields.keys())))
        write_json(leads_path, leads)
        return True, 'updated'

    if cmd == 'archive_lead':
        lid = payload.get('lead_id')
        if lid in leads:
            leads[lid]['status'] = 'archived'
            _append_history(leads[lid], actor, 'archive', '')
            write_json(leads_path, leads)
            return True, 'archived'
        return False, 'lead not found'

    # ------------------
    # Orders / Invoices
    # ------------------
    if cmd == 'create_order':
        lid = payload.get('lead_id')
        if lid not in leads:
            return False, 'lead not found'
        # enforce min cases per flavor
        peach = int(payload.get('peach_cases') or 0)
        cherry = int(payload.get('cherry_cases') or 0)
        if peach < 25 or cherry < 25:
            return False, 'min 25 cases per flavor'

        # pricing comes from settings (price_per_case)
        oid = _next_id('O', orders)
        orders[oid] = {
            'id': oid,
            'lead_id': lid,
            'client_id': '',
            'created_at': now_iso(),
            'items': [
                {'sku': 'PEACH_BLACK', 'cases': peach, 'cans_per_case': 24},
                {'sku': 'CHERRY_PINK', 'cases': cherry, 'cans_per_case': 24},
            ],
            'pricing': {},
            'status': 'draft',
            'delivery_date': '',
            'delivery_time': '',
            'total': (payload.get('total') or '').strip(),
            'created_by': actor,
            'printed': False,
        }
        orders[oid] = _recalc_order(orders[oid], settings)
        _append_history(leads[lid], actor, 'order_create', oid)
        write_json(orders_path, orders)
        write_json(leads_path, leads)
        return True, oid

    

    if cmd == 'update_order_fields':
        oid = payload.get('order_id')
        fields = payload.get('fields') or {}
        if oid not in orders:
            return False, 'order not found'

        # totals are auto-calculated (no manual total)


        # items cases update
        if 'peach_cases' in fields or 'cherry_cases' in fields:
            try:
                peach = int(fields.get('peach_cases') or orders[oid]['items'][0]['cases'])
                cherry = int(fields.get('cherry_cases') or orders[oid]['items'][1]['cases'])
            except Exception:
                return False, 'bad cases'
            if peach < 25 or cherry < 25:
                return False, 'min 25 cases per flavor'
            orders[oid]['items'][0]['cases'] = peach
            orders[oid]['items'][1]['cases'] = cherry

        if 'delivery_date' in fields:
            orders[oid]['delivery_date'] = (fields.get('delivery_date') or '').strip()
        if 'delivery_time' in fields:
            orders[oid]['delivery_time'] = (fields.get('delivery_time') or '').strip()

        orders[oid] = _recalc_order(orders[oid], settings)

        _append_history(orders[oid], actor, 'update', ','.join(sorted(fields.keys())))
        write_json(orders_path, orders)
        return True, 'order updated'

    if cmd == 'archive_order':
        oid = payload.get('order_id')
        if oid in orders:
            orders[oid]['status'] = 'archived'
            _append_history(orders[oid], actor, 'archive', '')
            write_json(orders_path, orders)
            return True, 'order archived'
        return False, 'order not found'

    if cmd == 'archive_client':
        cid = payload.get('client_id')
        if cid in clients:
            clients[cid]['archived'] = True
            _append_history(clients[cid], actor, 'archive', '')
            write_json(clients_path, clients)
            return True, 'client archived'
        return False, 'client not found'


    # ------------------
    # Calendar
    # ------------------
    if cmd == 'create_event':
        title = (payload.get('title') or '').strip()
        date_s = (payload.get('date') or '').strip()
        time_s = (payload.get('time') or '').strip()
        etype = (payload.get('type') or 'event').strip().lower()
        notes = (payload.get('notes') or '').strip()
        try:
            dur = int((payload.get('duration') or '30').strip())
        except Exception:
            dur = 30
        dur = max(5, min(dur, 24*60))
        if not title or not date_s or not time_s:
            return False, 'missing title/date/time'

        eid = _next_id('E', calendar)
        visible = ['admin']
        assign_to = (payload.get('assign_to') or '').strip()
        if actor and actor not in visible:
            visible.append(actor)
        if assign_to and assign_to not in visible:
            visible.append(assign_to)

        calendar[eid] = {
            'id': eid,
            'type': etype,
            'title': title,
            'date': date_s,
            'time': time_s,
            'duration_min': dur,
            'notes': notes,
            'created_by': actor,
            'visible_to': visible,
            'archived': False,
            'related': {},
            'created_at': now_iso(),
        }
        write_json(calendar_path, calendar)

        if actor and users.get(actor, {}).get('role') != 'admin':
            add_notif('event_created', f"Event {eid} created by {actor}", 'admin')
            write_json(notif_path, notifs)
        return True, 'event created'

    if cmd == 'archive_event':
        eid = payload.get('event_id')
        if eid in calendar:
            calendar[eid]['archived'] = True
            write_json(calendar_path, calendar)
            return True, 'event archived'
        return False, 'event not found'
    if cmd == 'generate_invoice_pdf':
        oid = payload.get('order_id')
        if oid not in orders:
            return False, 'order not found'
        order = orders[oid]
        lid = order.get('lead_id')
        if lid not in leads:
            return False, 'lead missing'
        lead = leads[lid]
        # require rep fields
        if not (lead.get('rep_name') and lead.get('rep_phone') and lead.get('rep_email')):
            return False, 'missing rep fields'
        iid = _next_id('I', invoices)
        invoice = {
            'id': iid,
            'order_id': oid,
            'created_at': now_iso(),
            'status': 'generated',
            'pdf_path': f"docs/invoices/{iid}.pdf",
            'bill_to_email': lead.get('rep_email',''),
        }
        invoices[iid] = invoice
        # generate doc
        base_dir = os.path.dirname(data_dir)
        abs_doc = os.path.join(data_dir, invoice['pdf_path'])
        generated = generate_invoice(abs_doc, invoice, order, lead)
        # if fallback html, store that
        if generated.endswith('.html'):
            invoice['pdf_path'] = os.path.relpath(generated, data_dir).replace('\\','/')
            invoices[iid] = invoice
        order['status'] = 'invoiced'
        orders[oid] = order
        write_json(invoices_path, invoices)
        write_json(orders_path, orders)
        return True, iid

    if cmd == 'mark_order_paid':
        oid = payload.get('order_id')
        if oid not in orders:
            return False, 'order not found'
        order = orders[oid]
        order['status'] = 'paid'

        lid = order.get('lead_id')
        lead = leads.get(lid)
        if lead:
            lead['status'] = 'paid'
            _append_history(lead, actor, 'paid', oid)

            cid = order.get('client_id')
            if not cid:
                cid = _next_id('C', clients)
                clients[cid] = {
                    'id': cid,
                    'created_at': now_iso(),
                    'lead_id': lid,
                    'business_name': lead.get('business_name',''),
                    'business_phone': lead.get('business_phone',''),
                    'business_address': lead.get('business_address',''),
                    'rep_name': lead.get('rep_name',''),
                    'rep_phone': lead.get('rep_phone',''),
                    'rep_email': lead.get('rep_email',''),
                    'rep_address': lead.get('rep_address',''),
                    'status': 'active',
                    'archived': False,
                    'history': [],
                }
                _append_history(clients[cid], actor, 'create_from_lead', lid)
                order['client_id'] = cid

        orders[oid] = _recalc_order(order, settings)
        write_json(orders_path, orders)
        write_json(leads_path, leads)
        write_json(clients_path, clients)
        add_notif('order_paid', f"{oid} marked as paid", 'admin', open_url=f"/order?id={oid}")
        write_json(notif_path, notifs)
        return True, 'paid'

    if cmd == 'schedule_delivery':
        oid = payload.get('order_id')
        date = (payload.get('date') or '').strip()
        time_s = (payload.get('time') or '').strip()
        if oid not in orders:
            return False, 'order not found'
        if orders[oid].get('status') != 'paid':
            return False, 'must be paid first'
        orders[oid]['status'] = 'scheduled'
        orders[oid]['delivery_date'] = date
        orders[oid]['delivery_time'] = time_s
        lid = orders[oid].get('lead_id')
        lead = leads.get(lid)
        if not lead:
            return False, 'lead missing'

        # mark lead scheduled
        lead['status'] = 'scheduled'
        _append_history(lead, actor, 'scheduled', f"{oid} {date} {time_s}")

        # convert lead -> client (only once)
        cid = orders[oid].get('client_id')
        if not cid:
            cid = _next_id('C', clients)
            clients[cid] = {
                'id': cid,
                'created_at': now_iso(),
                'lead_id': lid,
                'business_name': lead.get('business_name',''),
                'business_phone': lead.get('business_phone',''),
                'business_address': lead.get('business_address',''),
                'rep_name': lead.get('rep_name',''),
                'rep_phone': lead.get('rep_phone',''),
                'rep_email': lead.get('rep_email',''),
                'rep_address': lead.get('rep_address',''),
                'status': 'active',
                'archived': False,
            }
            orders[oid]['client_id'] = cid

        # create calendar event
        eid = _next_id('E', calendar)
        calendar[eid] = {
            'id': eid,
            'type': 'delivery',
            'date': date,
            'time': time_s,
            'title': f"Delivery - {lead.get('business_name','')}",
            'related': {'order_id': oid, 'lead_id': lid, 'client_id': cid},
            'created_by': actor,
            'visible_to': ['admin', actor, 'delivery'],
        }

        # admin notification to print order
        add_notif('delivery_scheduled', f"{oid} scheduled for {date} {time_s}", 'admin', open_url=f"/order?id={oid}")

        # write
        write_json(orders_path, orders)
        write_json(clients_path, clients)
        write_json(calendar_path, calendar)
        write_json(notif_path, notifs)
        return True, 'scheduled'

    if cmd == 'generate_order_pdf':
        oid = payload.get('order_id')
        if oid not in orders:
            return False, 'order not found'
        order = orders[oid]
        if order.get('status') not in ('scheduled','delivered'):
            return False, 'not scheduled'
        lid = order.get('lead_id')
        lead = leads.get(lid) or {}
        base_dir = os.path.dirname(data_dir)
        abs_doc = os.path.join(data_dir, 'docs', 'orders', f"{oid}.pdf")
        generated = generate_order(abs_doc, order, lead)
        # mark printed
        order['printed'] = True
        orders[oid] = order
        write_json(orders_path, orders)
        return True, os.path.relpath(generated, data_dir).replace('\\','/')

    if cmd == 'mark_delivered':
        oid = payload.get('order_id')
        if oid in orders:
            orders[oid]['status'] = 'delivered'
            write_json(orders_path, orders)
            return True, 'delivered'
        return False, 'order not found'

    return False, 'unknown cmd'