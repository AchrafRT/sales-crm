#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sales CRM (Deterministic, file-tree, stdlib server)

Run:
  python3 server.py
Then open:
  http://127.0.0.1:8000

Default admin:
  admin / admin
"""

from __future__ import annotations

import html
import json
import mimetypes
import os
import urllib.parse
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional, Tuple, List

from core.utils import read_json, write_json, ensure_dir, render_template, now_iso
from core.auth import hash_password, verify_password, new_session, get_session, delete_session, find_user, get_user_by_id
from core.command_bus import write_command
from core.worker import process_command_file
from core.permissions import can_view_lead, can_edit_lead, can_view_order, can_view_event
from core.xlsx_import import parse_leads_file, map_lead_fields

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
PUBLIC_DIR = os.path.join(BASE_DIR, 'public')


def _init_data() -> None:
    ensure_dir(DATA_DIR)
    ensure_dir(os.path.join(DATA_DIR, 'inbox'))
    ensure_dir(os.path.join(DATA_DIR, 'processed'))
    ensure_dir(os.path.join(DATA_DIR, 'logs'))
    ensure_dir(os.path.join(DATA_DIR, 'docs', 'invoices'))
    ensure_dir(os.path.join(DATA_DIR, 'docs', 'orders'))

    # json stores
    for name, default in [
        ('sessions.json', {}),
        ('users.json', {}),
        ('leads.json', {}),
        ('clients.json', {}),
        ('orders.json', {}),
        ('invoices.json', {}),
        ('calendar.json', {}),
        ('notifications.json', {}),
        ('settings.json', {'currency': 'CAD', 'price_per_case': 59.76, 'gst_rate': 0.05, 'qst_rate': 0.09975, 'company_name': 'SayF Sales', 'company_email': 'sales@example.com'}),
    ]:
        path = os.path.join(DATA_DIR, name)
        if not os.path.exists(path):
            write_json(path, default)

    users_path = os.path.join(DATA_DIR, 'users.json')
    users = read_json(users_path, {})
    if not any(u.get('role') == 'admin' for u in users.values()):
        users['U0001'] = {
            'id': 'U0001',
            'role': 'admin',
            'username': 'admin',
            'pass_hash': hash_password('admin'),
            'active': True,
            'needs_first_login': False,
            'created_at': now_iso(),
        }
        users['U0002'] = {
            'id': 'U0002',
            'role': 'employee',
            'username': 'employee',
            'pass_hash': hash_password('employee'),
            'active': True,
            'needs_first_login': True,
            'created_at': now_iso(),
        }
        write_json(users_path, users)

    # demo data (only if empty)
    leads_path = os.path.join(DATA_DIR, 'leads.json')
    leads = read_json(leads_path, {})
    if not leads:
        demo = [
            {'business_name': 'Depanneur A', 'business_phone': '514-000-0001', 'business_address': 'Montreal, QC'},
            {'business_name': 'Cafe B', 'business_phone': '514-000-0002', 'business_address': 'Laval, QC'},
            {'business_name': 'Restaurant C', 'business_phone': '514-000-0003', 'business_address': 'Longueuil, QC'},
        ]
        # import via worker command (so history is correct)
        cmd_path = write_command(DATA_DIR, 'import_leads_batch', 'U0001', {'rows': demo})
        process_command_file(BASE_DIR, cmd_path)
        # assign one lead to employee
        leads = read_json(leads_path, {})
        lids = sorted(leads.keys())
        if lids:
            cmd_path = write_command(DATA_DIR, 'assign_lead', 'U0001', {'lead_id': lids[0], 'user_id': 'U0002'})
            process_command_file(BASE_DIR, cmd_path)


def _read_template(name: str) -> str:
    with open(os.path.join(PUBLIC_DIR, name), 'r', encoding='utf-8') as f:
        return f.read()


def _cookie_get(handler: BaseHTTPRequestHandler, name: str) -> str:
    raw = handler.headers.get('Cookie', '')
    try:
        c = cookies.SimpleCookie(); c.load(raw)
        if name in c:
            return c[name].value
    except Exception:
        return ''
    return ''


def _set_cookie(handler: BaseHTTPRequestHandler, name: str, value: str, path: str = '/', max_age: int = 60*60*24*30) -> None:
    c = cookies.SimpleCookie()
    c[name] = value
    c[name]['path'] = path
    c[name]['max-age'] = str(max_age)
    handler.send_header('Set-Cookie', c.output(header='').strip())


def _theme_class(handler: BaseHTTPRequestHandler) -> str:
    t = _cookie_get(handler, 'THEME')
    return 'light' if t == 'light' else ''


# --- Language (EN/FR) ---
_T = {
    'en': {
        'Theme': 'Theme',
        'Logout': 'Logout',
        'LangEN': 'EN',
        'LangFR': 'FR',
        'Notifications': 'Notifications',
        'Leads': 'Leads',
        'Calendar': 'Calendar',
        'Clients': 'Clients',
        'Orders': 'Orders',
        'Employees': 'Employees',
        'Settings': 'Settings',
        'Search': 'Search',
        'IncludeArchive': 'Include archive',
        'Archive': 'Archive',
        'Archived': 'Archived',
        'Open': 'Open',
        'Create': 'Create',
        'Delete': 'Delete',
        'DeleteSelected': 'Delete selected',
        'ArchiveSelected': 'Archive selected',
        'CreateLead': 'Create new lead',
        'UploadLeads': 'Upload leads',
        'BulkAssign': 'Bulk assign',
        'AssignSelected': 'Assign selected',
        'ViewArchived': 'View archived',
        'HideArchived': 'Hide archived',
        'Day': 'Day',
        'Week': 'Week',
        'Month': 'Month',
        'Year': 'Year',
        'Time': 'Time',
        'Date': 'Date',
        'Title': 'Title',
        'Type': 'Type',
        'Print': 'Print',
        'Details': 'Details',
    },
    'fr': {
        'Theme': 'Thème',
        'Logout': 'Déconnexion',
        'LangEN': 'EN',
        'LangFR': 'FR',
        'Notifications': 'Notifications',
        'Leads': 'Prospects',
        'Calendar': 'Calendrier',
        'Clients': 'Clients',
        'Orders': 'Commandes',
        'Employees': 'Employés',
        'Settings': 'Réglages',
        'Search': 'Rechercher',
        'IncludeArchive': 'Inclure les archives',
        'Archive': 'Archiver',
        'Archived': 'Archivé',
        'Open': 'Ouvrir',
        'Create': 'Créer',
        'Delete': 'Supprimer',
        'DeleteSelected': 'Supprimer la sélection',
        'ArchiveSelected': 'Archiver la sélection',
        'CreateLead': 'Créer un prospect',
        'UploadLeads': 'Importer des prospects',
        'BulkAssign': 'Assigner en lot',
        'AssignSelected': 'Assigner la sélection',
        'ViewArchived': 'Voir les archives',
        'HideArchived': 'Masquer les archives',
        'Day': 'Jour',
        'Week': 'Semaine',
        'Month': 'Mois',
        'Year': 'Année',
        'Time': 'Heure',
        'Date': 'Date',
        'Title': 'Titre',
        'Type': 'Type',
        'Print': 'Imprimer',
        'Details': 'Détails',
    }
}

def _lang(handler: BaseHTTPRequestHandler) -> str:
    l = _cookie_get(handler, 'LANG')
    return 'fr' if l == 'fr' else 'en'

def _t(handler: BaseHTTPRequestHandler, key: str) -> str:
    l = _lang(handler)
    return _T.get(l, _T['en']).get(key, key)


def _flash(handler: BaseHTTPRequestHandler) -> str:
    msg = _cookie_get(handler, 'FLASH')
    if not msg:
        return ''
    # clear
    c = cookies.SimpleCookie(); c['FLASH'] = ''
    c['FLASH']['path'] = '/'
    c['FLASH']['max-age'] = '0'
    handler.send_header('Set-Cookie', c.output(header='').strip())
    # render
    return f"<div class='notice' style='margin:12px 0'>{html.escape(msg)}</div>"


def _set_flash(handler: BaseHTTPRequestHandler, msg: str) -> None:
    c = cookies.SimpleCookie(); c['FLASH'] = msg
    c['FLASH']['path'] = '/'
    c['FLASH']['max-age'] = str(30)
    handler.send_header('Set-Cookie', c.output(header='').strip())


def _parse_post(handler: BaseHTTPRequestHandler) -> Dict[str, str]:
    length = int(handler.headers.get('Content-Length', '0') or 0)
    body = handler.rfile.read(length) if length else b''
    ctype = handler.headers.get('Content-Type', '')
    if 'application/x-www-form-urlencoded' in ctype:
        qs = urllib.parse.parse_qs(body.decode('utf-8', errors='ignore'))
        return {k: (v[0] if v else '') for k, v in qs.items()}
    return {}


def _parse_multipart(handler: BaseHTTPRequestHandler) -> Tuple[Dict[str, str], Optional[Tuple[str, bytes]]]:
    """Returns (fields, file) where file=(filename, bytes) or None"""
    ctype = handler.headers.get('Content-Type', '')
    if 'multipart/form-data' not in ctype:
        return {}, None
    length = int(handler.headers.get('Content-Length', '0') or 0)
    body = handler.rfile.read(length) if length else b''

    # boundary
    parts = ctype.split('boundary=')
    if len(parts) < 2:
        return {}, None
    boundary = parts[1].strip().encode('utf-8')
    delim = b'--' + boundary

    fields: Dict[str, str] = {}
    file_out: Optional[Tuple[str, bytes]] = None

    chunks = body.split(delim)
    for ch in chunks:
        if not ch or ch in (b'--\r\n', b'--'):
            continue
        if ch.startswith(b'--'):
            continue
        # strip leading CRLF
        if ch.startswith(b'\r\n'):
            ch = ch[2:]
        header_blob, _, content = ch.partition(b'\r\n\r\n')
        content = content.rstrip(b'\r\n')
        headers = header_blob.decode('utf-8', errors='ignore').split('\r\n')
        dispo = ''
        for hline in headers:
            if hline.lower().startswith('content-disposition:'):
                dispo = hline
        if not dispo:
            continue
        # parse disposition
        # Content-Disposition: form-data; name="file"; filename="x.xlsx"
        name = ''
        filename = ''
        for seg in dispo.split(';'):
            seg = seg.strip()
            if seg.startswith('name='):
                name = seg.split('=',1)[1].strip().strip('"')
            if seg.startswith('filename='):
                filename = seg.split('=',1)[1].strip().strip('"')
        if filename:
            file_out = (filename, content)
        else:
            fields[name] = content.decode('utf-8', errors='ignore')

    return fields, file_out


def _require_user(handler: BaseHTTPRequestHandler) -> Optional[Dict[str, Any]]:
    sid = _cookie_get(handler, 'SID')
    sess = get_session(DATA_DIR, sid)
    if not sess:
        return None
    u = get_user_by_id(DATA_DIR, sess.get('user_id',''))
    if not u or not u.get('active', True):
        return None
    return u


def _redirect(handler: BaseHTTPRequestHandler, to: str) -> None:
    handler.send_response(302)
    handler.send_header('Location', to)
    handler.end_headers()


def _send_html(handler: BaseHTTPRequestHandler, html_text: str) -> None:
    b = html_text.encode('utf-8')
    handler.wfile.write(b)


def _send_file(handler: BaseHTTPRequestHandler, path: str) -> None:
    if not os.path.exists(path) or os.path.isdir(path):
        handler.send_error(404)
        return
    ctype, _ = mimetypes.guess_type(path)
    ctype = ctype or 'application/octet-stream'
    with open(path, 'rb') as f:
        data = f.read()
    handler.send_response(200)
    handler.send_header('Content-Type', ctype)
    handler.send_header('Content-Length', str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def _badge(status: str) -> str:
    s = (status or '').lower()
    cls = 'badge'
    if s in ('new','contacted','qualified','draft'):
        cls += ' orange'
    if s in ('invoiced','paid','scheduled'):
        cls += ' green'
    if s in ('archived',):
        cls += ' red'
    return f"<span class='{cls}'>{html.escape(s or '-')}</span>"


def _nav(handler: BaseHTTPRequestHandler, role: str, tab: str, base: str) -> str:
    items = []
    def t(label: str, key: str) -> None:
        active = 'active' if tab == key else ''
        items.append(f"<a class='tab {active}' href='{base}?tab={key}'>{label}</a>")

    t(_t(handler,'Notifications'),'notifications')
    t(_t(handler,'Leads'),'leads')
    t(_t(handler,'Calendar'),'calendar')
    t(_t(handler,'Clients'),'clients')
    t(_t(handler,'Orders'),'orders')
    if role == 'admin':
        t(_t(handler,'Employees'),'employees')
        t(_t(handler,'Settings'),'settings')
    else:
        t(_t(handler,'Settings'),'settings')
    return ''.join(items)


def _kpis_html(user: Dict[str, Any]) -> str:
    # very simple daily kpis based on today actions (demo-level)
    # In real use, you'd compute from history and timestamps.
    leads = read_json(os.path.join(DATA_DIR, 'leads.json'), {})
    orders = read_json(os.path.join(DATA_DIR, 'orders.json'), {})
    today = now_iso()[:10]
    leads_today = sum(1 for l in leads.values() if (l.get('created_at','')[:10] == today))
    invoiced_today = sum(1 for o in orders.values() if (o.get('status') == 'invoiced' and (o.get('created_at','')[:10] == today)))
    paid_today = sum(1 for o in orders.values() if (o.get('status') == 'paid'))
    closes_today = sum(1 for o in orders.values() if (o.get('status') == 'scheduled'))
    ad_spend_today = 0
    roas_today = '—'

    cards = [
        ('Leads Today', leads_today),
        ('Deposits Today', paid_today),
        ('Closes Today', closes_today),
        ('Ad Spend Today', ad_spend_today),
        ('ROAS Today', roas_today),
    ]
    out = ["<div class='kpis' style='margin-top:12px'>"]
    for lab, num in cards:
        out.append("<div class='kpi'>" +
                   f"<div class='num'>{html.escape(str(num))}</div>" +
                   f"<div class='lab'>{html.escape(lab)}</div>" +
                   "</div>")
    out.append("</div>")
    return ''.join(out)


def _page(handler: BaseHTTPRequestHandler, title: str, subtitle: str, nav_html: str, body_html: str, path_qs: str) -> None:
    base = _read_template('base.html')
    handler.send_response(200)
    handler.send_header('Content-Type', 'text/html; charset=utf-8')
    # flash cookie cleared inside _flash (needs header), so call after send_response but before end_headers
    ctx = {
        'title': title,
        'subtitle': subtitle,
        'nav': nav_html,
        'body': body_html,
        'kpis': '',
        'flash': '',
        'theme_class': _theme_class(handler),
        'path_qs': html.escape(path_qs),
    }
    # write headers for flash clear if any
    flash_html = _flash(handler)
    ctx['flash'] = flash_html
    handler.end_headers()
    _send_html(handler, render_template(base, ctx))


def _write_cmd_and_process(actor: str, cmd: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
    cmd_path = write_command(DATA_DIR, cmd, actor, payload)
    return process_command_file(BASE_DIR, cmd_path)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        q = urllib.parse.parse_qs(parsed.query)

        # static
        if path.startswith('/assets/'):
            return _send_file(self, os.path.join(PUBLIC_DIR, path.lstrip('/')))
        if path == '/styles.css':
            return _send_file(self, os.path.join(PUBLIC_DIR, 'styles.css'))

        # docs
        if path.startswith('/docs/'):
            u = _require_user(self)
            if not u:
                return _redirect(self, '/login')
            # allow admin or employee on their own orders/invoices (simple: allow all authenticated)
            return _send_file(self, os.path.join(DATA_DIR, path.replace('/docs/', 'docs/').lstrip('/')))

        if path == '/' or path == '':
            u = _require_user(self)
            if not u:
                return _redirect(self, '/login')
            if u['role'] == 'admin':
                return _redirect(self, '/admin')
            if u['role'] == 'employee':
                return _redirect(self, '/employee')
            return _redirect(self, '/delivery')

        if path == '/lang':
            next_url = (q.get('next', ['/' ])[0] or '/').strip()
            cur = _cookie_get(self, 'LANG')
            new = 'fr' if cur != 'fr' else 'en'
            self.send_response(302)
            _set_cookie(self, 'LANG', new)
            self.send_header('Location', next_url)
            self.end_headers()
            return

        if path == '/theme':
            # toggle
            next_url = (q.get('next', ['/' ])[0] or '/').strip()
            cur = _cookie_get(self, 'THEME')
            new = 'light' if cur != 'light' else 'dark'
            self.send_response(302)
            _set_cookie(self, 'THEME', new)
            self.send_header('Location', next_url)
            self.end_headers()
            return

        if path == '/login':
            theme = _theme_class(self)
            next_url = (q.get('next', ['/'])[0] or '/').strip()
            tpl = _read_template('login.html')
            lang = _lang(self)
            html_out = render_template(tpl, {
                'title': 'Sales CRM - Login' if lang=='en' else 'Sales CRM - Connexion',
                'subtitle': 'Admin / Employee access' if lang=='en' else 'Accès Admin / Employé',
                'theme_class': theme,
                'flash': '',
                'next': html.escape(next_url),
            'btn_lang': 'FR' if lang=='en' else 'EN',
            'btn_theme': _t(self,'Theme'),
                'lbl_user': 'Username' if lang=='en' else "Nom d'utilisateur",
                'lbl_pass': 'Password' if lang=='en' else 'Mot de passe',
                'btn_open': 'Open Portal' if lang=='en' else 'Ouvrir le portail',
                'hint_admin': 'Default admin: ' + '<b>admin / admin</b>' if lang=='en' else 'Admin par défaut : ' + '<b>admin / admin</b>',
                'hint_emp': 'Admin creates employee usernames/passwords. Employees sign in here.' if lang=='en' else "L'admin crée les accès des employés. Les employés se connectent ici.",
            })
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            flash_html = _flash(self)
            self.end_headers()
            html_out = html_out.replace('{{flash}}', flash_html)
            return _send_html(self, html_out)

        if path == '/logout':
            sid = _cookie_get(self, 'SID')
            if sid:
                delete_session(DATA_DIR, sid)
            self.send_response(302)
            _set_cookie(self, 'SID', '', max_age=0)
            self.send_header('Location', '/login')
            self.end_headers()
            return

        # protected pages
        u = _require_user(self)
        if not u:
            return _redirect(self, '/login')

        if path in ('/admin', '/employee', '/delivery'):
            if path == '/admin' and u['role'] != 'admin':
                return _redirect(self, '/')
            if path == '/employee' and u['role'] != 'employee':
                return _redirect(self, '/')
            if path == '/delivery' and u['role'] != 'delivery':
                return _redirect(self, '/')

            tab = (q.get('tab', ['notifications'])[0] or 'notifications').strip()
            base = path
            nav = _nav(self, u['role'], tab, base)
            body = _render_tab(self, u, tab, q)
            base_tpl = _read_template('base.html')

            ctx = {
                'title': f"Sales CRM - {u['role'].title()}",
                'subtitle': f"Logged in as {html.escape(u['username'])} ({u['role']})",
                'nav': nav,
                'body': body,
                'kpis': _kpis_html(u),
                'flash': '',
                'theme_class': _theme_class(self),
                'path_qs': html.escape(self.path),
            'btn_theme': html.escape(_t(self,'Theme')),
            'btn_logout': html.escape(_t(self,'Logout')),
            'btn_lang': html.escape('FR' if _lang(self)=='en' else 'EN'),
            }
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            flash_html = _flash(self)
            ctx['flash'] = flash_html
            self.end_headers()
            return _send_html(self, render_template(base_tpl, ctx))

        if path == '/lead':
            lid = (q.get('id', [''])[0] or '').strip()
            leads = read_json(os.path.join(DATA_DIR, 'leads.json'), {})
            lead = leads.get(lid)
            if not lead or not can_view_lead(u, lead):
                self.send_response(404); self.end_headers();
                return _send_html(self, '<h1>Not found</h1>')
            return _render_lead_page(self, u, lead)

        if path == '/order':
            oid = (q.get('id', [''])[0] or '').strip()
            orders = read_json(os.path.join(DATA_DIR, 'orders.json'), {})
            order = orders.get(oid)
            if not order or not can_view_order(u, order):
                self.send_response(404); self.end_headers();
                return _send_html(self, '<h1>Not found</h1>')
            return _render_order_page(self, u, order)

        if path == '/client':
            cid = (q.get('id', [''])[0] or '').strip()
            clients = read_json(os.path.join(DATA_DIR, 'clients.json'), {})
            client = clients.get(cid)
            if not client:
                self.send_response(404); self.end_headers();
                return _send_html(self, '<h1>Not found</h1>')
            return _render_client_page(self, u, client)



        if path == '/event':
            eid = (q.get('id', [''])[0] or '').strip()
            cal = read_json(os.path.join(DATA_DIR, 'calendar.json'), {})
            ev = cal.get(eid)
            if (not ev) or (not can_view_event(u, ev)):
                self.send_response(404); self.end_headers();
                return _send_html(self, '<h1>Not found</h1>')

            archive_btn = ''
            if u['role'] == 'admin' and not ev.get('archived'):
                archive_btn = (
                    "<form method='post' action='/action' style='display:inline' onsubmit=\"return confirm('Archive this event?')\">"
                    "<input type='hidden' name='cmd' value='archive_event'>"
                    f"<input type='hidden' name='event_id' value='{html.escape(ev.get('id',''))}'>"
                    f"<input type='hidden' name='next' value='/event?id={html.escape(ev.get('id',''))}'>"
                    "<button class='btn danger' type='submit'>Archive</button></form>"
                )

            event_info = (
                "<div class='grid'>"
                "<div class='card'>"
                "<h4 style='margin:0 0 10px 0'>When</h4>"
                f"<div><b>Date:</b> {html.escape(ev.get('date',''))}</div>"
                f"<div><b>Time:</b> {html.escape(ev.get('time',''))}</div>"
                f"<div><b>Duration:</b> {html.escape(str(ev.get('duration_min','')))} min</div>"
                "</div>"
                "<div class='card'>"
                "<h4 style='margin:0 0 10px 0'>Details</h4>"
                f"<div><b>Type:</b> {html.escape(ev.get('type',''))}</div>"
                f"<div><b>Notes:</b> {html.escape(ev.get('notes',''))}</div>"
                "</div>"
                "</div>"
            )

            body = render_template(_read_tpl('page_event.html'), {
                'back_url': f"/{u['role']}?tab=calendar",
                'back_label': 'Back',
                'event_title': html.escape(ev.get('title','')),
                'event_subtitle': f"{html.escape(ev.get('date',''))} {html.escape(ev.get('time',''))} • {html.escape(ev.get('type',''))}",
                'archive_btn': archive_btn,
                'event_info': event_info,
            })

            base_tpl = _read_template('base.html')
            ctx = {
                'title': 'Sales CRM - Event',
                'subtitle': f"Logged in as {html.escape(u['username'])} ({u['role']})",
                'nav': _nav(self, u['role'], 'calendar', f"/{u['role']}"),
                'body': body,
                'kpis': _kpis_html(u),
                'flash': _flash(self),
                'theme_class': _theme_class(self),
                'path_qs': html.escape(self.path),
                'btn_theme': html.escape(_t(self,'Theme')),
                'btn_logout': html.escape(_t(self,'Logout')),
                'btn_lang': html.escape('FR' if _lang(self)=='en' else 'EN'),
            }
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            return _send_html(self, render_template(base_tpl, ctx))

        if path == '/event_new':
            next_url = (q.get('next', [f"/{u['role']}?tab=calendar"])[0] or '').strip() or f"/{u['role']}?tab=calendar"
            users = read_json(os.path.join(DATA_DIR, 'users.json'), {})
            assignee_html = ''
            if u['role'] == 'admin':
                opts = ["<option value=''>—</option>"]
                for uid, uu in users.items():
                    if uu.get('role') in ('employee','delivery') and uu.get('active', True):
                        opts.append(f"<option value='{html.escape(uid)}'>{html.escape(uu.get('username',''))} ({html.escape(uu.get('role',''))})</option>")
                assignee_html = (
                    "<div class='field'><label class='small muted'>Assign to</label>"
                    "<select name='assign_to'>" + ''.join(opts) + "</select></div>"
                )

            body = render_template(_read_tpl('page_event_new.html'), {
                'title': html.escape(_t(self,'CreateEvent')),
                'back_url': html.escape(next_url),
                'back_label': html.escape(_t(self,'Back')),
                'next_url': html.escape(next_url),
                'assignees': assignee_html,
            })

            base_tpl = _read_template('base.html')
            ctx = {
                'title': 'Sales CRM - New Event',
                'subtitle': f"Logged in as {html.escape(u['username'])} ({u['role']})",
                'nav': _nav(self, u['role'], 'calendar', f"/{u['role']}"),
                'body': body,
                'kpis': _kpis_html(u),
                'flash': _flash(self),
                'theme_class': _theme_class(self),
                'path_qs': html.escape(self.path),
                'btn_theme': html.escape(_t(self,'Theme')),
                'btn_logout': html.escape(_t(self,'Logout')),
                'btn_lang': html.escape('FR' if _lang(self)=='en' else 'EN'),
            }
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            return _send_html(self, render_template(base_tpl, ctx))

        self.send_error(404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == '/login':
            form = _parse_post(self)
            username = (form.get('username') or '').strip()
            password = (form.get('password') or '').strip()
            next_url = (form.get('next') or '/').strip() or '/'
            u = find_user(DATA_DIR, username)
            if not u or not verify_password(password, u.get('pass_hash','')):
                self.send_response(302)
                _set_flash(self, 'Bad login')
                self.send_header('Location', '/login')
                self.end_headers()
                return
            sid = new_session(DATA_DIR, u['id'])
            self.send_response(302)
            _set_cookie(self, 'SID', sid)
            self.send_header('Location', next_url if next_url.startswith('/') else '/')
            self.end_headers()
            return

        u = _require_user(self)
        if not u:
            return _redirect(self, '/login')

        # generic command action
        if path == '/action':
            form = _parse_post(self)
            cmd = (form.get('cmd') or '').strip()
            next_url = (form.get('next') or '/').strip() or '/'

            ok = False
            msg = 'bad request'

            if cmd == 'create_employee' and u['role'] == 'admin':
                ok, msg = _write_cmd_and_process(u['id'], 'create_employee', {
                    'username': form.get('username',''),
                    'password': form.get('password',''),
                    'role': form.get('role','employee'),
                })

            elif cmd == 'disable_user' and u['role'] == 'admin':
                ok, msg = _write_cmd_and_process(u['id'], 'disable_user', {'user_id': form.get('user_id','')})

            elif cmd == 'reset_password' and u['role'] == 'admin':
                ok, msg = _write_cmd_and_process(u['id'], 'reset_password', {'user_id': form.get('user_id',''), 'password': form.get('password','')})

            elif cmd == 'assign_lead' and u['role'] == 'admin':
                ok, msg = _write_cmd_and_process(u['id'], 'assign_lead', {'lead_id': form.get('lead_id',''), 'user_id': form.get('user_id','')})

            elif cmd == 'create_lead':
                assigned_to = form.get('assigned_to','') if u['role']=='admin' else u['id']
                ok, msg = _write_cmd_and_process(u['id'], 'create_lead', {
                    'business_name': form.get('business_name',''),
                    'business_phone': form.get('business_phone',''),
                    'business_address': form.get('business_address',''),
                    'assigned_to': assigned_to,
                })

            elif cmd == 'delete_leads' and u['role'] == 'admin':
                lead_ids = [k for k, v in form.items() if k.startswith('lid_') and v == 'on']
                lead_ids = [x.replace('lid_', '') for x in lead_ids]
                ok, msg = _write_cmd_and_process(u['id'], 'delete_leads', {'lead_ids': lead_ids})

            elif cmd == 'archive_leads_bulk' and u['role'] == 'admin':
                lead_ids = [k for k, v in form.items() if k.startswith('lid_') and v == 'on']
                lead_ids = [x.replace('lid_', '') for x in lead_ids]
                n = 0
                for lid in lead_ids:
                    ok_i, _ = _write_cmd_and_process(u['id'], 'archive_lead', {'lead_id': lid})
                    if ok_i:
                        n += 1
                ok, msg = True, f'archived {n}'

            elif cmd == 'update_order_fields':
                oid = form.get('order_id','')
                orders = read_json(os.path.join(DATA_DIR, 'orders.json'), {})
                order = orders.get(oid)
                if order and can_view_order(u, order):
                    ok, msg = _write_cmd_and_process(u['id'], 'update_order_fields', {
                        'order_id': oid,
                        'fields': {
                            'peach_cases': form.get('peach_cases',''),
                            'cherry_cases': form.get('cherry_cases',''),
                            'delivery_date': form.get('delivery_date',''),
                            'delivery_time': form.get('delivery_time',''),
                        }
                    })
                else:
                    ok, msg = False, 'no permission'

            elif cmd == 'archive_order' and u['role'] == 'admin':
                ok, msg = _write_cmd_and_process(u['id'], 'archive_order', {'order_id': form.get('order_id','')})

            elif cmd == 'archive_client' and u['role'] == 'admin':
                ok, msg = _write_cmd_and_process(u['id'], 'archive_client', {'client_id': form.get('client_id','')})

            elif cmd == 'archive_event' and u['role'] == 'admin':
                ok, msg = _write_cmd_and_process(u['id'], 'archive_event', {'event_id': form.get('event_id','')})

            
            elif cmd == 'update_settings' and u['role'] == 'admin':
                ok, msg = _write_cmd_and_process(u['id'], 'update_settings', {
                    'company_name': form.get('company_name',''),
                    'company_email': form.get('company_email',''),
                    'currency': form.get('currency','CAD'),
                    'price_per_case': form.get('price_per_case','59.76'),
                    'gst_rate': form.get('gst_rate','0.05'),
                    'qst_rate': form.get('qst_rate','0.09975'),
                })
            elif cmd == 'create_event':
                payload = {
                    'title': form.get('title',''),
                    'date': form.get('date',''),
                    'time': form.get('time',''),
                    'type': form.get('type','event'),
                    'duration': form.get('duration','30'),
                    'notes': form.get('notes',''),
                }
                if u['role'] == 'admin':
                    payload['assign_to'] = form.get('assign_to','')
                ok, msg = _write_cmd_and_process(u['id'], 'create_event', payload)

            elif cmd == 'assign_leads_bulk' and u['role'] == 'admin':
                lead_ids = [k for k, v in form.items() if k.startswith('lid_') and v == 'on']
                lead_ids = [x.replace('lid_', '') for x in lead_ids]
                ok, msg = _write_cmd_and_process(u['id'], 'assign_leads_bulk', {'lead_ids': lead_ids, 'user_id': form.get('user_id','')})

            elif cmd == 'update_lead_fields':
                lid = form.get('lead_id','')
                leads = read_json(os.path.join(DATA_DIR, 'leads.json'), {})
                lead = leads.get(lid)
                if lead and can_edit_lead(u, lead):
                    fields = {
                        'rep_name': form.get('rep_name',''),
                        'rep_phone': form.get('rep_phone',''),
                        'rep_email': form.get('rep_email',''),
                        'rep_address': form.get('rep_address',''),
                        'notes': form.get('notes',''),
                        'status': form.get('status',''),
                    }
                    ok, msg = _write_cmd_and_process(u['id'], 'update_lead_fields', {'lead_id': lid, 'fields': fields})
                else:
                    ok, msg = False, 'no permission'

            elif cmd == 'archive_lead' and u['role'] == 'admin':
                ok, msg = _write_cmd_and_process(u['id'], 'archive_lead', {'lead_id': form.get('lead_id','')})

            elif cmd == 'create_order':
                lid = form.get('lead_id','')
                leads = read_json(os.path.join(DATA_DIR, 'leads.json'), {})
                lead = leads.get(lid)
                if lead and can_edit_lead(u, lead):
                    ok, msg = _write_cmd_and_process(u['id'], 'create_order', {
                        'lead_id': lid,
                        'peach_cases': form.get('peach_cases','0'),
                        'cherry_cases': form.get('cherry_cases','0'),
                    })
                else:
                    ok, msg = False, 'no permission'

            elif cmd == 'generate_invoice_pdf':
                oid = form.get('order_id','')
                orders = read_json(os.path.join(DATA_DIR, 'orders.json'), {})
                order = orders.get(oid)
                if order and can_view_order(u, order):
                    ok, msg = _write_cmd_and_process(u['id'], 'generate_invoice_pdf', {'order_id': oid})
                else:
                    ok, msg = False, 'no permission'

            elif cmd == 'mark_order_paid':
                oid = form.get('order_id','')
                orders = read_json(os.path.join(DATA_DIR, 'orders.json'), {})
                order = orders.get(oid)
                if order and can_view_order(u, order):
                    ok, msg = _write_cmd_and_process(u['id'], 'mark_order_paid', {'order_id': oid})
                else:
                    ok, msg = False, 'no permission'

            elif cmd == 'schedule_delivery':
                oid = form.get('order_id','')
                orders = read_json(os.path.join(DATA_DIR, 'orders.json'), {})
                order = orders.get(oid)
                if order and can_view_order(u, order):
                    ok, msg = _write_cmd_and_process(u['id'], 'schedule_delivery', {
                        'order_id': oid,
                        'date': form.get('date',''),
                        'time': form.get('time',''),
                    })
                else:
                    ok, msg = False, 'no permission'

            elif cmd == 'generate_order_pdf' and u['role'] == 'admin':
                oid = form.get('order_id','')
                ok, msg = _write_cmd_and_process(u['id'], 'generate_order_pdf', {'order_id': oid})

            else:
                ok, msg = False, 'blocked'

            self.send_response(302)
            _set_flash(self, ('OK: ' if ok else 'NO: ') + msg)
            self.send_header('Location', next_url)
            self.end_headers()
            return

        if path == '/upload_leads' and u['role'] == 'admin':
            fields, file_part = _parse_multipart(self)
            next_url = (fields.get('next') or '/admin?tab=leads').strip()
            if not file_part:
                self.send_response(302)
                _set_flash(self, 'NO: missing file')
                self.send_header('Location', next_url)
                self.end_headers()
                return
            filename, blob = file_part
            tmp_dir = os.path.join(DATA_DIR, '_uploads')
            ensure_dir(tmp_dir)
            tmp_path = os.path.join(tmp_dir, filename)
            with open(tmp_path, 'wb') as f:
                f.write(blob)

            rows_raw, mode = parse_leads_file(tmp_path)
            mapped = [map_lead_fields(r) for r in rows_raw]
            # keep only rows with at least name or phone
            mapped = [r for r in mapped if (r.get('business_name') or r.get('business_phone') or r.get('business_address'))]

            # chunk import for safer command size
            added = 0
            chunk: List[Dict[str, Any]] = []
            for r in mapped:
                chunk.append(r)
                if len(chunk) >= 250:
                    ok, msg = _write_cmd_and_process(u['id'], 'import_leads_batch', {'rows': chunk})
                    if ok:
                        added += len(chunk)
                    chunk = []
            if chunk:
                ok, msg = _write_cmd_and_process(u['id'], 'import_leads_batch', {'rows': chunk})
                if ok:
                    added += len(chunk)

            self.send_response(302)
            _set_flash(self, f'OK: imported {added} ({mode})')
            self.send_header('Location', next_url)
            self.end_headers()
            return

        self.send_error(404)

    # ---------------------
    # Rendering helpers
    # ---------------------


def _read_tpl(name: str) -> str:
    """Read HTML template from public/templates."""
    path = os.path.join(PUBLIC_DIR, 'templates', name)
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def _match_blob(qtxt: str, *parts: str) -> bool:
    if not qtxt:
        return True
    blob = ' '.join([(p or '') for p in parts]).lower()
    return qtxt in blob


def _search_bar(self, base: str, tab: str, qtxt: str, inc_arch: bool, arch_only: bool, include_archive_toggle: bool = True) -> str:
    qv = html.escape(qtxt or '')
    inc = 'checked' if inc_arch else ''
    arch = "<input type='hidden' name='arch_only' value='1'>" if arch_only else ''
    archive_bits = ''
    if include_archive_toggle:
        archive_bits = (
            "<label class='small muted' style='display:flex;align-items:center;gap:8px'>"
            f"<input type='checkbox' name='inc_arch' value='1' {inc}>"
            f"{html.escape(_t(self,'IncludeArchive'))}"
            "</label>"
        )
    return (
        "<form method='get' class='row' style='justify-content:space-between;margin:10px 0'>"
        f"<input type='hidden' name='tab' value='{html.escape(tab)}'>"
        f"{arch}"
        "<div class='row' style='gap:10px'>"
        f"<input name='q' placeholder='{html.escape(_t(self,'Search'))}' value='{qv}' style='min-width:240px'>"
        f"{archive_bits}"
        "</div>"
        "<button class='btn' type='submit'>" + html.escape(_t(self,'Search')) + "</button>"
        "</form>"
    )


def _render_tab(self, u: Dict[str, Any], tab: str, q: Dict[str, Any]) -> str:
    role = u['role']
    base = '/admin' if role == 'admin' else ('/employee' if role == 'employee' else '/delivery')

    qtxt = (q.get('q', [''])[0] or '').strip().lower()
    inc_arch = (q.get('inc_arch', [''])[0] in ('1', 'on', 'true', 'yes'))
    arch_only = (q.get('arch_only', [''])[0] in ('1', 'on', 'true', 'yes'))

    users = read_json(os.path.join(DATA_DIR, 'users.json'), {})
    # --- Notifications ---
    if tab == 'notifications':
        notifications = read_json(os.path.join(DATA_DIR, 'notifications.json'), [])
        # tolerate different shapes
        if isinstance(notifications, dict):
            notifications = notifications.get('notifications') or notifications.get('items') or []
        if not isinstance(notifications, list):
            notifications = []

        rows = []
        for n in reversed(notifications[-250:]):
            if not isinstance(n, dict):
                continue
            if not _match_blob(qtxt, n.get('type', ''), n.get('text', ''), n.get('at', ''), n.get('ref', '')):
                continue

            ref = n.get('ref') or ''
            open_btn = ''
            if ref.startswith('lead:'):
                lid = ref.split(':', 1)[1]
                open_btn = f"<a class='btn' href='/lead?id={urllib.parse.quote(lid)}'>{html.escape(_t(self,'Open'))}</a>"
            elif ref.startswith('order:'):
                oid = ref.split(':', 1)[1]
                open_btn = f"<a class='btn' href='/order?id={urllib.parse.quote(oid)}'>{html.escape(_t(self,'Open'))}</a>"
            elif ref.startswith('event:'):
                eid = ref.split(':', 1)[1]
                open_btn = f"<a class='btn' href='/event?id={urllib.parse.quote(eid)}'>{html.escape(_t(self,'Open'))}</a>"
            elif ref.startswith('client:'):
                cid = ref.split(':', 1)[1]
                open_btn = f"<a class='btn' href='/client?id={urllib.parse.quote(cid)}'>{html.escape(_t(self,'Open'))}</a>"

            at = html.escape(str(n.get('at', '')))
            typ = html.escape(str(n.get('type', '')))
            text = html.escape(str(n.get('text', '')))
            rows.append(f"<tr><td class='small muted'>{at}</td><td>{typ}</td><td>{text}</td><td>{open_btn}</td></tr>")

        if not rows:
            rows.append("<tr><td colspan='4' class='muted'>None.</td></tr>")

        tpl = _read_tpl('tab_notifications.html')
        return render_template(tpl, {
            'search_bar': _search_bar(self, base, tab, qtxt, False, False, include_archive_toggle=False),
            'title': html.escape(_t(self, 'Notifications')),
            'rows': ''.join(rows),
        })

    # --- Calendar ---
    if tab == 'calendar':
        calendar = read_json(os.path.join(DATA_DIR, 'calendar.json'), {})
        view = q.get('view', ['month'])[0]
        if view not in ('day', 'week', 'month', 'year'):
            view = 'month'

        events = []
        for ev in calendar.values():
            if not can_view_event(u, ev):
                continue
            if arch_only and not ev.get('archived'):
                continue
            if (not inc_arch) and ev.get('archived'):
                continue
            if not _match_blob(qtxt, ev.get('title',''), ev.get('type',''), ev.get('date',''), ev.get('time',''), ev.get('notes','')):
                continue
            events.append(ev)
        events.sort(key=lambda x: (x.get('date',''), x.get('time','')))

        controls = (
            "<div class='row' style='justify-content:space-between;align-items:center;margin-bottom:10px'>"
            "<div class='row' style='gap:8px'>"
            f"<a class='btn' href='{base}?tab=calendar&view=day'>Day</a>"
            f"<a class='btn' href='{base}?tab=calendar&view=week'>Week</a>"
            f"<a class='btn' href='{base}?tab=calendar&view=month'>Month</a>"
            f"<a class='btn' href='{base}?tab=calendar&view=year'>Year</a>"
            f"<a class='btn' href='{base}?tab=calendar&view={view}&arch_only=1&inc_arch=1'>{html.escape(_t(self,'ViewArchived'))}</a>"
            f"<a class='btn' href='{base}?tab=calendar&view={view}'>{html.escape(_t(self,'HideArchived'))}</a>"
            "</div>"
            f"<a class='btn primary' href='/event_new?next={urllib.parse.quote(base + '?tab=calendar')}'>{html.escape(_t(self,'CreateEvent'))}</a>"
            "</div>"
        )

        # simple agenda list (UI preserved)
        cards = []
        for ev in events[:800]:
            kind = html.escape(ev.get('type',''))
            title = html.escape(ev.get('title',''))
            dt = f"{html.escape(ev.get('date',''))} {html.escape(ev.get('time',''))}"
            notes = html.escape(ev.get('notes',''))
            cards.append(
                "<div class='card' style='margin:10px 0'>"
                + f"<div class='row' style='justify-content:space-between;align-items:center'>"
                + f"<div><b>{title}</b><div class='muted small'>{dt} • {kind}</div></div>"
                + f"<a class='btn' href='/event?id={urllib.parse.quote(ev.get('id',''))}'>{html.escape(_t(self,'Open'))}</a>"
                + "</div>"
                + ((f"<div class='muted small' style='margin-top:8px'>{notes}</div>") if notes else '')
                + "</div>"
            )
        if not cards:
            cards.append("<div class='muted'>None.</div>")

        tpl = _read_tpl('tab_calendar.html')
        return render_template(tpl, {
            'search_bar': _search_bar(self, base, tab, qtxt, inc_arch, arch_only, include_archive_toggle=True),
            'controls': controls,
            'title': html.escape(_t(self, 'Calendar')),
            'events_html': ''.join(cards),
        })

    # --- Clients ---
    if tab == 'clients':
        clients = read_json(os.path.join(DATA_DIR, 'clients.json'), {})
        rows = []
        for c in clients.values():
            if arch_only and not c.get('archived'):
                continue
            if (not inc_arch) and c.get('archived'):
                continue
            if not _match_blob(qtxt, c.get('id',''), c.get('business_name',''), c.get('rep_name',''), c.get('rep_email','')):
                continue
            rows.append(
                "<tr>"
                f"<td>{html.escape(c.get('id',''))}</td>"
                f"<td>{html.escape(c.get('business_name',''))}</td>"
                f"<td>{html.escape(c.get('rep_name',''))}<div class='muted small'>{html.escape(c.get('rep_email',''))}</div></td>"
                f"<td><a class='btn' href='/client?id={urllib.parse.quote(c.get('id',''))}'>{html.escape(_t(self,'Open'))}</a></td>"
                "</tr>"
            )
        if not rows:
            rows.append("<tr><td colspan='4' class='muted'>None.</td></tr>")

        top_controls = (
            "<div class='row' style='justify-content:space-between;margin:10px 0'>"
            "<div class='row' style='gap:8px'>"
            f"<a class='btn' href='{base}?tab=clients&arch_only=1&inc_arch=1'>{html.escape(_t(self,'ViewArchived'))}</a>"
            f"<a class='btn' href='{base}?tab=clients'>{html.escape(_t(self,'HideArchived'))}</a>"
            "</div></div>"
        )

        tpl = _read_tpl('tab_clients.html')
        return render_template(tpl, {
            'top_controls': top_controls,
            'search_bar': _search_bar(self, base, tab, qtxt, inc_arch, arch_only, include_archive_toggle=True),
            'title': html.escape(_t(self, 'Clients')),
            'rows': ''.join(rows),
        })

    # --- Orders ---
    if tab == 'orders':
        orders = read_json(os.path.join(DATA_DIR, 'orders.json'), {})
        leads = read_json(os.path.join(DATA_DIR, 'leads.json'), {})
        rows = []
        for o in orders.values():
            if not can_view_order(u, o):
                continue
            if arch_only and not o.get('archived'):
                continue
            if (not inc_arch) and o.get('archived'):
                continue
            lead = leads.get(o.get('lead_id',''), {})
            biz = lead.get('business_name','')
            if not _match_blob(qtxt, o.get('id',''), o.get('status',''), biz, o.get('delivery_date',''), o.get('delivery_time','')):
                continue
            rows.append(
                "<tr>"
                f"<td>{html.escape(o.get('id',''))}</td>"
                f"<td>{_badge(o.get('status',''))}</td>"
                f"<td>{html.escape(biz)}</td>"
                f"<td>{html.escape(o.get('delivery_date',''))} {html.escape(o.get('delivery_time',''))}</td>"
                f"<td><a class='btn' href='/order?id={urllib.parse.quote(o.get('id',''))}'>{html.escape(_t(self,'Open'))}</a></td>"
                "</tr>"
            )
        if not rows:
            rows.append("<tr><td colspan='5' class='muted'>None.</td></tr>")

        top_controls = (
            "<div class='row' style='justify-content:space-between;margin:10px 0'>"
            "<div class='row' style='gap:8px'>"
            f"<a class='btn' href='{base}?tab=orders&arch_only=1&inc_arch=1'>{html.escape(_t(self,'ViewArchived'))}</a>"
            f"<a class='btn' href='{base}?tab=orders'>{html.escape(_t(self,'HideArchived'))}</a>"
            "</div></div>"
        )

        tpl = _read_tpl('tab_orders.html')
        return render_template(tpl, {
            'top_controls': top_controls,
            'search_bar': _search_bar(self, base, tab, qtxt, inc_arch, arch_only, include_archive_toggle=True),
            'title': html.escape(_t(self, 'Orders')),
            'rows': ''.join(rows),
        })

    # --- Employees (admin only) ---
    if tab == 'employees':
        if role != 'admin':
            return "<div class='card'><div class='muted'>Admin only.</div></div>"

        users = read_json(os.path.join(DATA_DIR, 'users.json'), {})
        # create form
        create_form = (
            "<form method='post' action='/action'>"
            "<input type='hidden' name='cmd' value='create_employee'>"
            f"<input type='hidden' name='next' value='{base}?tab=employees'>"
            "<div class='field'><label class='small muted'>Username</label><input name='username' required></div>"
            "<div class='field'><label class='small muted'>Password</label><input name='password' required></div>"
            "<div class='field'><label class='small muted'>Role</label>"
            "<select name='role'><option value='employee'>employee</option><option value='delivery'>delivery</option><option value='admin'>admin</option></select></div>"
            "<button class='btn primary' type='submit'>Create</button>"
            "</form>"
        )

        rows = []
        for uid, usr in users.items():
            if uid == 'admin' and usr.get('role') == 'admin':
                pass
            if not _match_blob(qtxt, uid, usr.get('username',''), usr.get('role','')):
                continue
            active = usr.get('active', True)
            status = 'active' if active else 'disabled'

            actions = []
            if uid != 'admin':
                if active:
                    actions.append(
                        "<form method='post' action='/action' style='display:inline'>"
                        "<input type='hidden' name='cmd' value='disable_user'>"
                        f"<input type='hidden' name='user_id' value='{html.escape(uid)}'>"
                        f"<input type='hidden' name='next' value='{base}?tab=employees'>"
                        "<button class='btn danger' type='submit' onclick=\"return confirm('Disable this user?')\">Disable</button>"
                        "</form>"
                    )
                actions.append(
                    "<form method='post' action='/action' style='display:inline;margin-left:6px'>"
                    "<input type='hidden' name='cmd' value='reset_password'>"
                    f"<input type='hidden' name='user_id' value='{html.escape(uid)}'>"
                    f"<input type='hidden' name='next' value='{base}?tab=employees'>"
                    "<input name='password' placeholder='New pass' required style='width:120px'>"
                    "<button class='btn' type='submit'>Set</button>"
                    "</form>"
                )

            actions_html = ''.join(actions) if actions else "<span class='muted small'>&mdash;</span>"

            rows.append(
                "<tr>"
                + f"<td class='small muted'>{html.escape(uid)}</td>"
                + f"<td>{html.escape(usr.get('username',''))}</td>"
                + f"<td>{html.escape(usr.get('role',''))}</td>"
                + f"<td>{html.escape(status)}</td>"
                + f"<td>{actions_html}</td>"
                + "</tr>"
            )
        if not rows:
            rows.append("<tr><td colspan='5' class='muted'>None.</td></tr>")

        tpl = _read_tpl('tab_employees.html')
        return render_template(tpl, {
            'search_bar': _search_bar(self, base, tab, qtxt, False, False, include_archive_toggle=False),
            'create_title': 'Create employee',
            'create_form': create_form,
            'list_title': 'Employees',
            'rows': ''.join(rows),
        })

    # --- Settings ---
    if tab == 'settings':
        settings = read_json(os.path.join(DATA_DIR, 'settings.json'), {})
        if role == 'admin':
            form_html = (
                "<form method='post' action='/action'>"
                "<input type='hidden' name='cmd' value='update_settings'>"
                f"<input type='hidden' name='next' value='{base}?tab=settings'>"
                "<div class='grid'>"
                "<div class='card'>"
                "<h4 style='margin:0 0 10px 0'>Pricing</h4>"
                "<div class='field'><label class='small muted'>Price per case (24 cans)</label><input name='price_per_case' value='" + html.escape(str(settings.get('price_per_case', 59.76))) + "'></div>"
                "<div class='field'><label class='small muted'>Cans per case</label><input name='cans_per_case' value='" + html.escape(str(settings.get('cans_per_case', 24))) + "'></div>"
                "</div>"
                "<div class='card'>"
                "<h4 style='margin:0 0 10px 0'>Taxes (Quebec)</h4>"
                "<div class='field'><label class='small muted'>GST rate</label><input name='gst_rate' value='" + html.escape(str(settings.get('gst_rate', 0.05))) + "'></div>"
                "<div class='field'><label class='small muted'>QST rate</label><input name='qst_rate' value='" + html.escape(str(settings.get('qst_rate', 0.09975))) + "'></div>"
                "<div class='muted small'>GST 5% + QST 9.975% = 14.975%</div>"
                "</div>"
                "</div>"
                "<button class='btn primary' type='submit'>Save</button>"
                "</form>"
            )
            tpl = _read_tpl('tab_settings_admin.html')
            return render_template(tpl, {'title': 'Settings', 'form_html': form_html})
        else:
            info_html = (
                "<div class='muted'>Only admin can change settings.</div>"
                "<div style='margin-top:10px'>"
                f"<div><b>Price per case:</b> {html.escape(str(settings.get('price_per_case', 59.76)))}</div>"
                f"<div><b>Cans per case:</b> {html.escape(str(settings.get('cans_per_case', 24)))}</div>"
                f"<div><b>GST:</b> {html.escape(str(settings.get('gst_rate', 0.05)))}</div>"
                f"<div><b>QST:</b> {html.escape(str(settings.get('qst_rate', 0.09975)))}</div>"
                "</div>"
            )
            tpl = _read_tpl('tab_settings_view.html')
            return render_template(tpl, {'title': 'Settings', 'info_html': info_html})

    # --- Leads (default) ---
    # existing: admin sees all open leads; employee sees assigned; delivery sees none.
    leads = read_json(os.path.join(DATA_DIR, 'leads.json'), {})

    view = []
    for lead in leads.values():
        if not can_view_lead(u, lead):
            continue
        is_arch = bool(lead.get('archived'))
        if arch_only and not is_arch:
            continue
        if (not inc_arch) and is_arch:
            continue
        # hide paid/scheduled from leads list
        if not arch_only:
            st = (lead.get('status') or '').lower()
            if st in ('paid', 'scheduled', 'delivered'):
                continue
        if not _match_blob(qtxt, lead.get('id',''), lead.get('status',''), lead.get('business_name',''), lead.get('business_phone',''), lead.get('business_address',''), lead.get('rep_name',''), lead.get('rep_email','')):
            continue
        view.append(lead)
    view.sort(key=lambda x: (x.get('status',''), x.get('created_at','')), reverse=True)

    tools_html = ''
    if role == 'admin':
        emp_opts = [f"<option value='{uid}'>{html.escape(v.get('username',''))} ({v.get('role')})</option>" for uid, v in users.items() if v.get('role') in ('employee','delivery') and v.get('active', True)]
        tools_html = (
            "<div class='grid'>"
            "<div class='card'>"
            f"<h3 style='margin:0 0 10px 0'>{html.escape(_t(self,'CreateLead'))}</h3>"
            "<form method='post' action='/action'>"
            "<input type='hidden' name='cmd' value='create_lead'/>"
            f"<input type='hidden' name='next' value='{base}?tab=leads'/>"
            "<div class='field'><label class='small muted'>Business name</label><input name='business_name' required></div>"
            "<div class='field'><label class='small muted'>Business phone</label><input name='business_phone'></div>"
            "<div class='field'><label class='small muted'>Business address</label><input name='business_address'></div>"
            "<div class='field'><label class='small muted'>Assign to</label><select name='assigned_to'><option value=''>—</option>" + ''.join(emp_opts) + "</select></div>"
            f"<button class='btn primary' type='submit'>{html.escape(_t(self,'Create'))}</button>"
            "</form>"
            "</div>"
            "<div class='card'>"
            f"<h3 style='margin:0 0 10px 0'>{html.escape(_t(self,'UploadLeads'))}.xlsx</h3>"
            "<form method='post' action='/upload_leads' enctype='multipart/form-data'>"
            f"<input type='hidden' name='next' value='{base}?tab=leads' />"
            "<div class='field'><input type='file' name='file' accept='.xlsx,.csv' required></div>"
            "<button class='btn primary' type='submit'>Upload</button>"
            "<div class='muted small' style='margin-top:8px'>If openpyxl is missing, upload CSV.</div>"
            "</form>"
            "</div>"
            "</div>"
        )

    top_controls = (
        "<div class='row' style='justify-content:space-between;margin-bottom:10px'>"
        "<div class='row' style='gap:8px'>"
        f"<a class='btn' href='{base}?tab=leads&arch_only=1&inc_arch=1'>{html.escape(_t(self,'ViewArchived'))}</a>"
        f"<a class='btn' href='{base}?tab=leads'>{html.escape(_t(self,'HideArchived'))}</a>"
        "</div>"
        "</div>"
    )

    table_head = "<tr>" + ("<th></th>" if role == 'admin' else "") + "<th>ID</th><th>Status</th><th>Business</th><th>Phone</th><th>Assigned</th><th></th></tr>"
    rows = []
    for lead in view[:700]:
        assigned = users.get(lead.get('assigned_to',''), {}).get('username', '-')
        chk = f"<td><input type='checkbox' name='lid_{html.escape(lead.get('id',''))}'></td>" if role == 'admin' else ''
        rows.append(
            "<tr>" +
            chk +
            f"<td>{html.escape(lead.get('id',''))}</td>" +
            f"<td>{_badge(lead.get('status',''))}</td>" +
            f"<td>{html.escape(lead.get('business_name',''))}<div class='muted small'>{html.escape(lead.get('business_address',''))}</div></td>" +
            f"<td>{html.escape(lead.get('business_phone',''))}</td>" +
            f"<td>{html.escape(assigned)}</td>" +
            f"<td><a class='btn' href='/lead?id={urllib.parse.quote(lead.get('id',''))}'>{html.escape(_t(self,'Open'))}</a></td>" +
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='7' class='muted'>None.</td></tr>")

    bulk_actions = ''
    if role == 'admin':
        emp_opts2 = [f"<option value='{uid}'>{html.escape(v.get('username',''))} ({v.get('role')})</option>" for uid, v in users.items() if v.get('role') in ('employee','delivery') and v.get('active', True)]
        bulk_actions = (
            "<div class='row' style='justify-content:space-between;align-items:flex-end;margin-top:10px'>"
            "<div class='row' style='gap:10px;flex-wrap:wrap'>"
            "<div class='field' style='min-width:260px'><label class='small muted'>Assign selected</label>"
            "<select name='user_id'><option value=''>—</option>" + ''.join(emp_opts2) + "</select></div>"
            "</div>"
            "<div class='row' style='gap:8px;flex-wrap:wrap'>"
            "<button class='btn' type='submit' name='cmd' value='assign_leads_bulk'>Assign</button>"
            "<button class='btn' type='submit' name='cmd' value='archive_leads_bulk' onclick=\"return confirm('Archive selected leads?')\">Archive</button>"
            "<button class='btn danger' type='submit' name='cmd' value='delete_leads' onclick=\"return confirm('Delete selected leads? This cannot be undone.')\">Delete</button>"
            "</div>"
            "</div>"
        )

    tpl = _read_tpl('tab_leads.html')
    return render_template(tpl, {
        'tools_html': tools_html,
        'top_controls': top_controls,
        'search_bar': _search_bar(self, base, 'leads', qtxt, inc_arch, arch_only, include_archive_toggle=True),
        'title': html.escape(_t(self, 'Leads')),
        'next_url': f"{base}?tab=leads" + ("&inc_arch=1" if inc_arch else "") + ("&arch_only=1" if arch_only else "") + (f"&q={urllib.parse.quote(qtxt)}" if qtxt else ""),
        'table_head': table_head,
        'table_rows': ''.join(rows),
        'bulk_actions': bulk_actions,
    })



def _render_lead_page(self, u: Dict[str, Any], lead: Dict[str, Any]) -> Any:
    """Lead detail page (template-based)."""
    lid = lead.get('id', '')
    users = read_json(os.path.join(DATA_DIR, 'users.json'), {})
    orders = read_json(os.path.join(DATA_DIR, 'orders.json'), {})
    invoices = read_json(os.path.join(DATA_DIR, 'invoices.json'), {})
    settings = read_json(os.path.join(DATA_DIR, 'settings.json'), {})

    can_edit = can_edit_lead(u, lead)

    # pricing
    try:
        price_per_case = float(settings.get('price_per_case', 59.76))
    except Exception:
        price_per_case = 59.76
    try:
        cans_per_case = int(float(settings.get('cans_per_case', 24)))
    except Exception:
        cans_per_case = 24
    price_per_can = price_per_case / max(1, cans_per_case)

    # header
    assigned = users.get(lead.get('assigned_to', ''), {}).get('username', '-')
    header = (
        "<div class='card' style='margin-top:12px'>"
        f"<div class='row' style='justify-content:space-between;align-items:flex-start'>"
        f"<div><h2 style='margin:0'>Lead {html.escape(lid)}</h2>"
        f"<div class='muted small'>{html.escape(lead.get('business_name',''))}</div></div>"
        f"<div class='muted small'>Assigned: {html.escape(assigned)}</div>"
        f"</div>"
        "<hr>"
        f"<div class='row' style='gap:16px;flex-wrap:wrap'>"
        f"<div><b>Phone:</b> {html.escape(lead.get('business_phone',''))}</div>"
        f"<div><b>Address:</b> {html.escape(lead.get('business_address',''))}</div>"
        f"<div><b>Status:</b> {_badge(lead.get('status',''))}</div>"
        "</div>"
        "</div>"
    )

    # update form
    update_form = ""
    if can_edit:
        status_opts = ['new','called','followup','invoice_sent','paid','scheduled','delivered']
        st = (lead.get('status') or 'new')
        st_opts_html = "".join([f"<option value='{html.escape(x)}' {'selected' if x==st else ''}>{html.escape(x)}</option>" for x in status_opts])
        update_form = (
            "<div class='card' style='margin-top:12px'>"
            "<h3 style='margin:0 0 10px 0'>Lead info</h3>"
            "<form method='post' action='/action'>"
            "<input type='hidden' name='cmd' value='update_lead_fields'>"
            f"<input type='hidden' name='lead_id' value='{html.escape(lid)}'>"
            f"<input type='hidden' name='next' value='/lead?id={html.escape(lid)}'>"
            "<div class='grid'>"
            "<div>"
            "<div class='field'><label class='small muted'>Rep name</label><input name='rep_name' value='" + html.escape(lead.get('rep_name','')) + "'></div>"
            "<div class='row' style='gap:10px'>"
            "<div class='field' style='flex:1'><label class='small muted'>Rep phone</label><input name='rep_phone' value='" + html.escape(lead.get('rep_phone','')) + "'></div>"
            "<div class='field' style='flex:1'><label class='small muted'>Rep email</label><input name='rep_email' value='" + html.escape(lead.get('rep_email','')) + "'></div>"
            "</div>"
            "<div class='field'><label class='small muted'>Rep address</label><input name='rep_address' value='" + html.escape(lead.get('rep_address','')) + "'></div>"
            "</div>"
            "<div>"
            "<div class='field'><label class='small muted'>Status</label><select name='status'>" + st_opts_html + "</select></div>"
            "<div class='field'><label class='small muted'>Notes</label><textarea name='notes' rows='6'>" + html.escape(lead.get('notes','')) + "</textarea></div>"
            "</div>"
            "</div>"
            "<button class='btn primary' type='submit'>Save</button>"
            "</form>"
            "</div>"
        )

    # create order form
    order_form = ""
    if can_edit:
        order_form = (
            "<div class='card' style='margin-top:12px'>"
            "<h3 style='margin:0 0 10px 0'>Create order</h3>"
            "<form method='post' action='/action'>"
            "<input type='hidden' name='cmd' value='create_order'>"
            f"<input type='hidden' name='lead_id' value='{html.escape(lid)}'>"
            f"<input type='hidden' name='next' value='/lead?id={html.escape(lid)}'>"
            "<div class='row' style='gap:10px;flex-wrap:wrap'>"
            "<div class='field' style='min-width:220px'><label class='small muted'>Peach cases</label><input name='peach_cases' type='number' min='25' value='25'></div>"
            "<div class='field' style='min-width:220px'><label class='small muted'>Cherry cases</label><input name='cherry_cases' type='number' min='25' value='25'></div>"
            f"<div class='field' style='min-width:220px'><label class='small muted'>Price per case ({cans_per_case} cans)</label><input value='{price_per_case:.2f}' readonly></div>"
            f"<div class='field' style='min-width:220px'><label class='small muted'>Price per can</label><input value='{price_per_can:.2f}' readonly></div>"
            "</div>"
            "<div class='muted small' style='margin-top:6px'>Taxes: GST 5% + QST 9.975% (QC) will be added automatically.</div>"
            "<button class='btn primary' type='submit' style='margin-top:10px'>Create order</button>"
            "</form>"
            "</div>"
        )

    # related orders
    lead_orders = [o for o in orders.values() if o.get('lead_id') == lid]
    lead_orders.sort(key=lambda x: x.get('created_at',''), reverse=True)

    order_rows = []
    for o in lead_orders:
        oid = o.get('id','')
        inv_links = [iv for iv in invoices.values() if iv.get('order_id') == oid]
        inv_html = ''
        for iv in inv_links:
            pth = iv.get('pdf_path','')
            href = '/' + pth if pth.startswith('docs/') else '/docs/' + pth
            inv_html += f"<div class='small'>Invoice: <a href='{html.escape(href)}'>{html.escape(iv.get('id',''))}</a></div>"

        actions = []
        if can_view_order(u, o):
            actions.append(
                "<form method='post' action='/action' style='display:inline'>"
                "<input type='hidden' name='cmd' value='generate_invoice_pdf'>"
                f"<input type='hidden' name='order_id' value='{html.escape(oid)}'>"
                f"<input type='hidden' name='next' value='/lead?id={html.escape(lid)}'>"
                "<button class='btn' type='submit'>Invoice</button></form>"
            )
            actions.append(
                "<form method='post' action='/action' style='display:inline'>"
                "<input type='hidden' name='cmd' value='mark_order_paid'>"
                f"<input type='hidden' name='order_id' value='{html.escape(oid)}'>"
                f"<input type='hidden' name='next' value='/lead?id={html.escape(lid)}'>"
                "<button class='btn' type='submit'>Mark paid</button></form>"
            )
            actions.append(
                "<form method='post' action='/action' style='display:inline'>"
                "<input type='hidden' name='cmd' value='schedule_delivery'>"
                f"<input type='hidden' name='order_id' value='{html.escape(oid)}'>"
                f"<input type='hidden' name='next' value='/lead?id={html.escape(lid)}'>"
                "<input name='date' type='date' required style='padding:8px;border-radius:10px;border:1px solid var(--line);background:transparent;color:inherit'>"
                "<input name='time' type='time' required style='padding:8px;border-radius:10px;border:1px solid var(--line);background:transparent;color:inherit'>"
                "<button class='btn primary' type='submit'>Schedule</button></form>"
            )

        order_rows.append(
            "<tr>"
            f"<td>{html.escape(oid)}</td>"
            f"<td>{_badge(o.get('status',''))}</td>"
            f"<td>{html.escape(o.get('delivery_date',''))} {html.escape(o.get('delivery_time',''))}</td>"
            f"<td>{html.escape(str(o.get('total','')))}</td>"
            f"<td>{inv_html}</td>"
            f"<td>{' '.join(actions)}</td>"
            "</tr>"
        )
    if not order_rows:
        order_rows.append("<tr><td colspan='6' class='muted'>No orders yet.</td></tr>")

    # email box
    email_box = ''
    if can_edit:
        email_subject = f"Invoice for {lead.get('business_name','')}"
        email_body = (
            f"Hi {lead.get('rep_name','') or ''},\n\n"
            f"Attached is your invoice for the order (Peach + Cherry cans).\n"
            f"Once payment is confirmed, we will schedule delivery.\n\n"
            f"Thanks,\nSales Team\n"
        )
        email_box = (
            "<div class='card' style='margin-top:12px'>"
            "<h3 style='margin:0 0 10px 0'>Email template</h3>"
            f"<div class='field'><label class='small muted'>Subject</label><input value='{html.escape(email_subject)}' readonly></div>"
            f"<div class='field'><label class='small muted'>Body</label><textarea rows='7' readonly>{html.escape(email_body)}</textarea></div>"
            "<div class='muted small'>Copy/paste to your email, attach the invoice PDF link above.</div>"
            "</div>"
        )

    archive_box = ''
    if u['role'] == 'admin' and not lead.get('archived'):
        archive_box = (
            "<div class='card' style='margin-top:12px'>"
            "<form method='post' action='/action' onsubmit='return confirm(\"Archive this lead?\")'>"
            "<input type='hidden' name='cmd' value='archive_lead'>"
            f"<input type='hidden' name='lead_id' value='{html.escape(lid)}'>"
            f"<input type='hidden' name='next' value='/lead?id={html.escape(lid)}'>"
            "<button class='btn danger' type='submit'>Archive lead</button>"
            "</form>"
            "</div>"
        )

    # audit
    history = lead.get('history') or []
    audit_rows = []
    for h in reversed(history[-50:]):
        audit_rows.append(
            f"<tr><td>{html.escape(h.get('at',''))}</td><td>{html.escape(h.get('actor',''))}</td><td>{html.escape(h.get('action',''))}</td><td class='muted'>{html.escape(h.get('detail',''))}</td></tr>"
        )
    if not audit_rows:
        audit_rows.append("<tr><td colspan='4' class='muted'>None.</td></tr>")

    body = render_template(_read_tpl('page_lead.html'), {
        'back_url': f"/{u['role']}?tab=leads",
        'back_label': 'Back',
        'header': header,
        'update_form': update_form,
        'order_form': order_form,
        'orders_title': 'Orders',
        'order_rows': ''.join(order_rows),
        'email_box': email_box,
        'archive_box': archive_box,
        'audit_title': 'Audit log (last 50)',
        'audit_rows': ''.join(audit_rows),
    })

    base_tpl = _read_template('base.html')
    ctx = {
        'title': f"Sales CRM - Lead {lid}",
        'subtitle': f"Lead detail ({u['role']})",
        'nav': _nav(self, u['role'], 'leads', f"/{u['role']}") ,
        'body': body,
        'kpis': _kpis_html(u),
        'flash': _flash(self),
        'theme_class': _theme_class(self),
        'path_qs': html.escape(self.path),
        'btn_theme': html.escape(_t(self,'Theme')),
        'btn_logout': html.escape(_t(self,'Logout')),
        'btn_lang': html.escape('FR' if _lang(self)=='en' else 'EN'),
    }
    self.send_response(200)
    self.send_header('Content-Type', 'text/html; charset=utf-8')
    self.end_headers()
    return _send_html(self, render_template(base_tpl, ctx))



def _render_client_page(self, u: Dict[str, Any], client: Dict[str, Any]) -> Any:
    cid = client.get('id','')
    orders = read_json(os.path.join(DATA_DIR, 'orders.json'), {})

    related = []
    for o in orders.values():
        if o.get('client_id') != cid:
            continue
        if not can_view_order(u, o):
            continue
        related.append(o)
    related.sort(key=lambda x: x.get('created_at',''), reverse=True)

    order_rows = []
    for o in related:
        oid = o.get('id','')
        order_rows.append(
            "<tr>"
            f"<td>{html.escape(oid)}</td>"
            f"<td>{_badge(o.get('status',''))}</td>"
            f"<td>{html.escape(o.get('delivery_date',''))} {html.escape(o.get('delivery_time',''))}</td>"
            f"<td>{html.escape(str(o.get('total','')))}</td>"
            f"<td><a class='btn' href='/order?id={urllib.parse.quote(oid)}'>{html.escape(_t(self,'Open'))}</a></td>"
            "</tr>"
        )
    if not order_rows:
        order_rows.append("<tr><td colspan='5' class='muted'>No orders.</td></tr>")

    archive_btn = ''
    if u['role'] == 'admin' and not client.get('archived'):
        archive_btn = (
            "<form method='post' action='/action' style='display:inline' onsubmit=\"return confirm('Archive this client?')\">"
            "<input type='hidden' name='cmd' value='archive_client'>"
            f"<input type='hidden' name='client_id' value='{html.escape(cid)}'>"
            f"<input type='hidden' name='next' value='/client?id={html.escape(cid)}'>"
            "<button class='btn danger' type='submit'>Archive</button>"
            "</form>"
        )

    client_info = (
        "<div class='grid'>"
        "<div class='card'>"
        "<h4 style='margin:0 0 10px 0'>Business</h4>"
        f"<div><b>Name:</b> {html.escape(client.get('business_name',''))}</div>"
        f"<div><b>Phone:</b> {html.escape(client.get('business_phone',''))}</div>"
        f"<div><b>Address:</b> {html.escape(client.get('business_address',''))}</div>"
        "</div>"
        "<div class='card'>"
        "<h4 style='margin:0 0 10px 0'>Representative</h4>"
        f"<div><b>Name:</b> {html.escape(client.get('rep_name',''))}</div>"
        f"<div><b>Email:</b> {html.escape(client.get('rep_email',''))}</div>"
        f"<div><b>Phone:</b> {html.escape(client.get('rep_phone',''))}</div>"
        "</div>"
        "</div>"
    )

    body = render_template(_read_tpl('page_client.html'), {
        'back_url': f"/{u['role']}?tab=clients",
        'back_label': 'Back',
        'client_title': f"Client {html.escape(cid)}",
        'client_subtitle': html.escape(client.get('business_name','')),
        'archive_btn': archive_btn,
        'client_info': client_info,
        'orders_title': 'Orders',
        'order_rows': ''.join(order_rows),
    })

    base_tpl = _read_template('base.html')
    ctx = {
        'title': f"Sales CRM - Client {cid}",
        'subtitle': f"Client detail ({u['role']})",
        'nav': _nav(self, u['role'], 'clients', f"/{u['role']}") ,
        'body': body,
        'kpis': _kpis_html(u),
        'flash': _flash(self),
        'theme_class': _theme_class(self),
        'path_qs': html.escape(self.path),
        'btn_theme': html.escape(_t(self,'Theme')),
        'btn_logout': html.escape(_t(self,'Logout')),
        'btn_lang': html.escape('FR' if _lang(self)=='en' else 'EN'),
    }
    self.send_response(200)
    self.send_header('Content-Type', 'text/html; charset=utf-8')
    self.end_headers()
    return _send_html(self, render_template(base_tpl, ctx))



def _render_order_page(self, u: Dict[str, Any], order: Dict[str, Any]) -> Any:
    oid = order.get('id','')
    leads = read_json(os.path.join(DATA_DIR, 'leads.json'), {})
    clients = read_json(os.path.join(DATA_DIR, 'clients.json'), {})
    invoices = read_json(os.path.join(DATA_DIR, 'invoices.json'), {})

    lead = leads.get(order.get('lead_id',''), {})
    client = clients.get(order.get('client_id',''), {})

    items_html = (
        "<div class='grid'>"
        "<div class='card'>"
        "<h4 style='margin:0 0 10px 0'>Items</h4>"
        f"<div><b>Peach cases:</b> {html.escape(str(order.get('peach_cases','')))}</div>"
        f"<div><b>Cherry cases:</b> {html.escape(str(order.get('cherry_cases','')))}</div>"
        f"<div class='muted small' style='margin-top:8px'>Each case is {html.escape(str(order.get('cans_per_case',24)))} cans.</div>"
        "</div>"
        "<div class='card'>"
        "<h4 style='margin:0 0 10px 0'>Totals</h4>"
        f"<div><b>Subtotal:</b> {html.escape(str(order.get('subtotal','')))}</div>"
        f"<div><b>GST:</b> {html.escape(str(order.get('gst','')))}</div>"
        f"<div><b>QST:</b> {html.escape(str(order.get('qst','')))}</div>"
        f"<div><b>Total:</b> {html.escape(str(order.get('total','')))}</div>"
        "</div>"
        "</div>"
    )

    invoice_rows = [iv for iv in invoices.values() if iv.get('order_id') == oid]
    invoice_rows.sort(key=lambda x: x.get('created_at',''), reverse=True)
    if invoice_rows:
        links = []
        for iv in invoice_rows:
            pth = iv.get('pdf_path','')
            href = '/' + pth if pth.startswith('docs/') else '/docs/' + pth
            links.append(f"<div><a class='btn' href='{html.escape(href)}'>Download {html.escape(iv.get('id',''))}</a> <span class='muted small'>{html.escape(iv.get('created_at',''))}</span></div>")
        invoices_html = ''.join(links)
    else:
        invoices_html = "<div class='muted'>No invoices yet.</div>"

    right_actions = ''
    if u['role'] in ('admin','employee'):
        right_actions += (
            "<form method='post' action='/action' style='display:inline'>"
            "<input type='hidden' name='cmd' value='generate_invoice_pdf'>"
            f"<input type='hidden' name='order_id' value='{html.escape(oid)}'>"
            f"<input type='hidden' name='next' value='/order?id={html.escape(oid)}'>"
            "<button class='btn' type='submit'>Generate invoice</button></form>"
        )
        if order.get('status') != 'paid':
            right_actions += (
                "<form method='post' action='/action' style='display:inline;margin-left:6px'>"
                "<input type='hidden' name='cmd' value='mark_order_paid'>"
                f"<input type='hidden' name='order_id' value='{html.escape(oid)}'>"
                f"<input type='hidden' name='next' value='/order?id={html.escape(oid)}'>"
                "<button class='btn' type='submit'>Mark paid</button></form>"
            )
    if u['role'] == 'admin' and not order.get('archived'):
        right_actions += (
            "<form method='post' action='/action' style='display:inline;margin-left:6px' onsubmit=\"return confirm('Archive this order?')\">"
            "<input type='hidden' name='cmd' value='archive_order'>"
            f"<input type='hidden' name='order_id' value='{html.escape(oid)}'>"
            f"<input type='hidden' name='next' value='/order?id={html.escape(oid)}'>"
            "<button class='btn danger' type='submit'>Archive</button></form>"
        )

    order_info = (
        "<div class='grid'>"
        "<div class='card'>"
        "<h4 style='margin:0 0 10px 0'>Delivery</h4>"
        f"<div><b>Date:</b> {html.escape(order.get('delivery_date',''))}</div>"
        f"<div><b>Time:</b> {html.escape(order.get('delivery_time',''))}</div>"
        "</div>"
        "<div class='card'>"
        "<h4 style='margin:0 0 10px 0'>Business</h4>"
        f"<div><b>Name:</b> {html.escape(lead.get('business_name','') or client.get('business_name',''))}</div>"
        f"<div><b>Phone:</b> {html.escape(lead.get('business_phone','') or client.get('business_phone',''))}</div>"
        "</div>"
        "</div>"
    )

    body = render_template(_read_tpl('page_order.html'), {
        'back_url': f"/{u['role']}?tab=orders",
        'back_label': 'Back',
        'order_title': f"Order {html.escape(oid)}",
        'order_subtitle': f"Status: {html.escape(order.get('status',''))}",
        'right_actions': right_actions,
        'order_info': order_info,
        'items_title': 'Order',
        'items_html': items_html,
        'invoices_title': 'Invoices',
        'invoices_html': invoices_html,
    })

    base_tpl = _read_template('base.html')
    ctx = {
        'title': f"Sales CRM - Order {oid}",
        'subtitle': f"Order detail ({u['role']})",
        'nav': _nav(self, u['role'], 'orders', f"/{u['role']}") ,
        'body': body,
        'kpis': _kpis_html(u),
        'flash': _flash(self),
        'theme_class': _theme_class(self),
        'path_qs': html.escape(self.path),
        'btn_theme': html.escape(_t(self,'Theme')),
        'btn_logout': html.escape(_t(self,'Logout')),
        'btn_lang': html.escape('FR' if _lang(self)=='en' else 'EN'),
    }
    self.send_response(200)
    self.send_header('Content-Type', 'text/html; charset=utf-8')
    self.end_headers()
    return _send_html(self, render_template(base_tpl, ctx))

def main() -> None:
    _init_data()
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', '8000'))
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"Sales CRM running on http://127.0.0.1:{port} (or http://{host}:{port})")
    httpd.serve_forever()


if __name__ == '__main__':
    main()
