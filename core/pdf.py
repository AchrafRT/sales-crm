#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PDF generation.

If reportlab is available, we generate real PDFs.
If not, we generate a simple HTML file the user can print to PDF.
"""

from __future__ import annotations

import os
from typing import Dict, Any, List


def _try_reportlab():
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
        return letter, canvas
    except Exception:
        return None, None


def generate_invoice(doc_path: str, invoice: Dict[str, Any], order: Dict[str, Any], lead: Dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(doc_path), exist_ok=True)
    letter, canvas_mod = _try_reportlab()
    if canvas_mod:
        c = canvas_mod.Canvas(doc_path, pagesize=letter)
        w, h = letter
        y = h - 60
        c.setFont('Helvetica-Bold', 16)
        c.drawString(50, y, 'INVOICE')
        y -= 30
        c.setFont('Helvetica', 10)
        c.drawString(50, y, f"Invoice ID: {invoice['id']}")
        y -= 14
        c.drawString(50, y, f"Order ID: {order['id']}")
        y -= 14
        c.drawString(50, y, f"Date: {invoice.get('created_at','')}")
        y -= 24
        c.setFont('Helvetica-Bold', 12)
        c.drawString(50, y, 'Bill To')
        y -= 16
        c.setFont('Helvetica', 10)
        c.drawString(50, y, lead.get('business_name',''))
        y -= 14
        c.drawString(50, y, lead.get('business_address',''))
        y -= 14
        c.drawString(50, y, lead.get('business_phone',''))
        y -= 22
        c.setFont('Helvetica-Bold', 12)
        c.drawString(50, y, 'Items')
        y -= 18
        c.setFont('Helvetica', 10)
        items: List[Dict[str, Any]] = order.get('items') or []
        currency = (order.get('pricing') or {}).get('currency') or ''
        for it in items:
            sku = it.get('sku')
            cases = int(it.get('cases') or 0)
            cans_per_case = int(it.get('cans_per_case') or 24)
            ppc = float(it.get('price_per_can') or (order.get('pricing') or {}).get('price_per_can') or 0.0)
            ppcase = float(it.get('price_per_case') or (ppc * cans_per_case))
            line_total = float(it.get('line_total') or (ppcase * cases))
            line = f"{sku} — {cases} cases ({cans_per_case} cans/case)  |  {ppc:.2f}/can  |  {ppcase:.2f}/case  |  {line_total:.2f} {currency}".strip()
            c.drawString(60, y, line)
            y -= 14
        y -= 10
        c.setFont('Helvetica-Bold', 12)
        totals = order.get('totals') or {}
        cur = (order.get('pricing') or {}).get('currency') or ''
        try:
            subtotal = float(totals.get('subtotal') or 0.0)
        except Exception:
            subtotal = 0.0
        try:
            gst = float(totals.get('gst') or 0.0)
        except Exception:
            gst = 0.0
        try:
            qst = float(totals.get('qst') or 0.0)
        except Exception:
            qst = 0.0
        try:
            total = float(totals.get('total') or order.get('total_amount') or 0.0)
        except Exception:
            total = 0.0
        c.drawString(50, y, f'Subtotal: {subtotal:.2f} {cur}')
        y -= 14
        c.drawString(50, y, f'GST (5%): {gst:.2f} {cur}')
        y -= 14
        c.drawString(50, y, f'QST (9.975%): {qst:.2f} {cur}')
        y -= 14
        c.drawString(50, y, f'Total: {total:.2f} {cur}')
        y -= 30
        c.setFont('Helvetica', 9)
        c.drawString(50, y, 'Payment: e-transfer / card (record payment in CRM).')
        c.showPage()
        c.save()
        return doc_path

    # Fallback HTML
    html_path = os.path.splitext(doc_path)[0] + '.html'
    currency = (order.get('pricing') or {}).get('currency') or ''
    def _item_li(it: Dict[str, Any]) -> str:
        sku = it.get('sku') or ''
        cases = int(it.get('cases') or 0)
        cans_per_case = int(it.get('cans_per_case') or 24)
        ppc = float(it.get('price_per_can') or (order.get('pricing') or {}).get('price_per_can') or 0.0)
        ppcase = float(it.get('price_per_case') or (ppc * cans_per_case))
        line_total = float(it.get('line_total') or (ppcase * cases))
        return (
            f"<li><b>{sku}</b> — {cases} cases × {cans_per_case} cans &nbsp; "
            f"| <span class='muted'>{ppc:.2f}/can</span> "
            f"| <span class='muted'>{ppcase:.2f}/case</span> "
            f"| <b>{line_total:.2f} {currency}</b></li>"
        )
    items_html = ''.join([_item_li(it) for it in (order.get('items') or [])])
    body = f"""<!doctype html>
<html><head><meta charset='utf-8'><title>Invoice {invoice['id']}</title>
<style>body{{font-family:Arial,sans-serif;margin:40px}} .box{{border:1px solid #ddd;padding:16px;border-radius:10px}}</style>
</head><body>
<h1>Invoice</h1>
<div class='box'>
<p><b>Invoice ID:</b> {invoice['id']}<br>
<b>Order ID:</b> {order['id']}<br>
<b>Date:</b> {invoice.get('created_at','')}</p>
<h3>Bill To</h3>
<p>{lead.get('business_name','')}<br>{lead.get('business_address','')}<br>{lead.get('business_phone','')}</p>
<h3>Items</h3>
<ul>{items_html}</ul>
<p><b>Subtotal:</b> {float((order.get('totals') or {}).get('subtotal') or 0):.2f} {currency}<br>
<b>GST (5%):</b> {float((order.get('totals') or {}).get('gst') or 0):.2f} {currency}<br>
<b>QST (9.975%):</b> {float((order.get('totals') or {}).get('qst') or 0):.2f} {currency}<br>
<b>Total:</b> {float((order.get('totals') or {}).get('total') or (order.get('total_amount') or 0)):.2f} {currency}</p>
<p style='color:#666'>Print this page to PDF if needed.</p>
</div>
</body></html>"""
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(body)
    return html_path


def generate_order(doc_path: str, order: Dict[str, Any], lead: Dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(doc_path), exist_ok=True)
    letter, canvas_mod = _try_reportlab()
    if canvas_mod:
        c = canvas_mod.Canvas(doc_path, pagesize=letter)
        w, h = letter
        y = h - 60
        c.setFont('Helvetica-Bold', 16)
        c.drawString(50, y, 'ORDER / PICK LIST')
        y -= 28
        c.setFont('Helvetica', 10)
        c.drawString(50, y, f"Order ID: {order['id']}")
        y -= 14
        c.drawString(50, y, f"Delivery: {order.get('delivery_date','')} {order.get('delivery_time','')}")
        y -= 20
        c.setFont('Helvetica-Bold', 12)
        c.drawString(50, y, 'Client')
        y -= 16
        c.setFont('Helvetica', 10)
        c.drawString(50, y, lead.get('business_name',''))
        y -= 14
        c.drawString(50, y, lead.get('business_address',''))
        y -= 24
        c.setFont('Helvetica-Bold', 12)
        c.drawString(50, y, 'Items')
        y -= 18
        c.setFont('Helvetica', 10)
        for it in (order.get('items') or []):
            c.drawString(60, y, f"{it.get('sku')} — {it.get('cases')} cases")
            y -= 14
        y -= 10
        c.setFont('Helvetica-Bold', 12)
        c.drawString(50, y, f"Total: {order.get('total','')}")
        c.showPage(); c.save()
        return doc_path

    html_path = os.path.splitext(doc_path)[0] + '.html'
    items_html = ''.join([f"<li><b>{it.get('sku')}</b> — {it.get('cases')} cases</li>" for it in (order.get('items') or [])])
    body = f"""<!doctype html><html><head><meta charset='utf-8'><title>Order {order['id']}</title></head>
<body style='font-family:Arial;margin:40px'>
<h1>Order / Pick List</h1>
<p><b>Order:</b> {order['id']}<br>
<b>Delivery:</b> {order.get('delivery_date','')} {order.get('delivery_time','')}</p>
<h3>Client</h3>
<p>{lead.get('business_name','')}<br>{lead.get('business_address','')}</p>
<h3>Items</h3><ul>{items_html}</ul>
<p><b>Total:</b> {order.get('total','')}</p>
<p style='color:#666'>Print to PDF if needed.</p>
</body></html>"""
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(body)
    return html_path
