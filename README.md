# Sales CRM (Deterministic / File-Tree)

## Run locally

```bash
python3 server.py
```

Open:
- http://127.0.0.1:8000

## Default accounts

- Admin: `admin / admin`
- Employee (demo): `employee / employee`

## What it does (MVP)

- Admin:
  - Upload `leads.xlsx` (or `leads.csv`)
  - Bulk assign leads to employees
  - View notifications, leads, clients, orders, calendar
  - Print order once paid + scheduled
  - Create/disable employees

- Employee:
  - Only sees assigned leads
  - Fill rep info
  - Create order (min 25 cases per flavor)
  - Generate invoice (PDF if reportlab exists; HTML fallback)
  - Mark paid
  - Schedule delivery (creates calendar event + admin notification)

## Architecture

- JSON storage in `data/*.json`
- Every user action writes a command into `data/inbox/`
- The server processes the command immediately and moves it to `data/processed/`

No external DB.

## Notes

- For `.xlsx` import, the app uses `openpyxl` if it is available in your environment.
  If not, export CSV and upload that.
- For PDF invoices/orders, the app uses `reportlab` if available.
  Otherwise it generates an HTML document you can print to PDF.
