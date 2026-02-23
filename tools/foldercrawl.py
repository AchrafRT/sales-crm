#!/usr/bin/env python3
# Offline folder crawler for deterministic HTML/CSS template projects (no JS required)
# - Walks the project folder
# - Parses .html/.htm files for href/src/action
# - Flags missing *static* file refs
# - Treats {{placeholders}} refs as dynamic (not missing)
#
# Usage:
#   python tools/foldercrawl.py . --out folder_report.txt
#
import re
import argparse
from pathlib import Path
from urllib.parse import urlparse

RE_HREF   = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.I)
RE_SRC    = re.compile(r'src\s*=\s*["\']([^"\']+)["\']', re.I)
RE_ACTION = re.compile(r'action\s*=\s*["\']([^"\']+)["\']', re.I)
RE_FORM   = re.compile(r'<form\b[^>]*>', re.I)
RE_ENDFORM= re.compile(r'</form\s*>', re.I)
RE_BUTTON = re.compile(r'<button\b[^>]*>', re.I)
RE_SUBMIT = re.compile(r'<input\b[^>]*type\s*=\s*["\']submit["\'][^>]*>', re.I)
RE_PLACEH = re.compile(r'{{\s*[^}]+\s*}}')

def is_external(u: str) -> bool:
    u = (u or "").strip()
    if not u: return True
    if u.startswith(("#", "mailto:", "javascript:")): return True
    p = urlparse(u)
    return bool(p.scheme) and p.scheme.lower() in ("http", "https")

def is_dynamic(u: str) -> bool:
    u = (u or "")
    return "{{" in u and "}}" in u

def norm_target(u: str) -> str:
    return (u or "").strip()

def resolve_file_target(base_file: Path, target: str, root: Path):
    t = norm_target(target)
    if not t: return ("empty", None)
    if is_external(t): return ("external", None)
    if is_dynamic(t): return ("dynamic", t)
    if t.startswith("/"):
        return ("route", t)
    t0 = t.split("#", 1)[0].split("?", 1)[0]
    cand = (base_file.parent / t0).resolve()
    try:
        cand.relative_to(root.resolve())
    except Exception:
        return ("file_outside_root", cand)
    return ("file", cand)

def scan_html_file(fp: Path, root: Path):
    txt = fp.read_text(encoding="utf-8", errors="ignore")
    hrefs = RE_HREF.findall(txt)
    srcs = RE_SRC.findall(txt)
    actions = RE_ACTION.findall(txt)

    form_opens = len(RE_FORM.findall(txt))
    form_closes = len(RE_ENDFORM.findall(txt))
    buttons = len(RE_BUTTON.findall(txt))
    submits = len(RE_SUBMIT.findall(txt))

    placeholders = RE_PLACEH.findall(txt)
    has_backslash_quotes = ('\\"' in txt)

    targets = []
    for t in hrefs: targets.append(("href", t))
    for t in srcs: targets.append(("src", t))
    for t in actions: targets.append(("action", t))

    resolved = []
    for kind, t in targets:
        k, res = resolve_file_target(fp, t, root)
        resolved.append((kind, t, k, res))

    return {
        "file": fp,
        "href_count": len(hrefs),
        "src_count": len(srcs),
        "action_count": len(actions),
        "form_opens": form_opens,
        "form_closes": form_closes,
        "buttons": buttons,
        "submits": submits,
        "placeholders": placeholders,
        "has_backslash_quotes": has_backslash_quotes,
        "resolved": resolved,
    }

def main():
    ap = argparse.ArgumentParser(description="Offline crawl of HTML templates -> deterministic TXT report")
    ap.add_argument("root", nargs="?", default=".", help="Project root folder (default: .)")
    ap.add_argument("--out", default="folder_report.txt", help="Output TXT file")
    ap.add_argument("--ext", default=".html,.htm", help="Comma-separated HTML extensions")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    exts = {e.strip().lower() for e in args.ext.split(",") if e.strip()}

    html_files = sorted([p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in exts])

    pages = []
    missing_files = []
    outside_refs = []
    routes = set()
    externals = 0
    dynamic_refs = []
    placeholders_pages = []
    suspicious_pages = []
    dead_button_pages = []
    referenced_files = set()

    for fp in html_files:
        info = scan_html_file(fp, root)
        pages.append(info)

        if info["placeholders"]:
            placeholders_pages.append((fp, len(info["placeholders"])))
        if info["has_backslash_quotes"]:
            suspicious_pages.append(fp)
        if info["buttons"] > 0 and info["form_opens"] == 0 and info["submits"] == 0:
            dead_button_pages.append(fp)

        for kind, raw, k, res in info["resolved"]:
            if k == "route":
                routes.add(res)
            elif k == "external":
                externals += 1
            elif k == "dynamic":
                dynamic_refs.append((fp, kind, raw))
            elif k == "file":
                referenced_files.add(res)
                if not res.exists():
                    missing_files.append((fp, kind, raw, res))
            elif k == "file_outside_root":
                outside_refs.append((fp, kind, raw, res))

    orphans = [fp for fp in html_files if fp.resolve() not in referenced_files]

    out = Path(args.out).resolve()
    with out.open("w", encoding="utf-8") as f:
        f.write("=== FOLDER CRAWL REPORT (TEMPLATE-AWARE) ===\n")
        f.write(f"ROOT: {root}\n")
        f.write(f"HTML FILES: {len(html_files)}\n")
        f.write(f"UNIQUE ROUTES (server): {len(routes)}\n")
        f.write(f"EXTERNAL REFS (http/https): {externals}\n")
        f.write(f"DYNAMIC REFS ({{{{...}}}} in href/src/action): {len(dynamic_refs)}\n")
        f.write(f"PAGES WITH {{placeholders}}: {len(placeholders_pages)}\n")
        f.write(f"PAGES WITH \\\" (suspicious escaping): {len(suspicious_pages)}\n")
        f.write(f"HEURISTIC DEAD-BUTTON PAGES: {len(dead_button_pages)}\n")
        f.write(f"MISSING STATIC FILE REFS: {len(missing_files)}\n")
        f.write(f"OUTSIDE-ROOT FILE REFS: {len(outside_refs)}\n")
        f.write(f"ORPHAN HTML FILES (not linked via static href/src): {len(orphans)}\n\n")

        if routes:
            f.write("---- ROUTES DISCOVERED ----\n")
            for r in sorted(routes): f.write(r + "\n")
            f.write("\n")

        if dynamic_refs:
            f.write("---- DYNAMIC REFS (EXPECTED IN TEMPLATES) ----\n")
            for src, kind, raw in dynamic_refs[:300]:
                f.write(f"{src.relative_to(root)} :: {kind}='{raw}'\n")
            if len(dynamic_refs) > 300:
                f.write(f"... ({len(dynamic_refs)-300} more)\n")
            f.write("\n")

        if placeholders_pages:
            f.write("---- PAGES WITH {{placeholders}} ----\n")
            for fp, n in sorted(placeholders_pages, key=lambda x: (-x[1], str(x[0]))):
                f.write(f"{fp.relative_to(root)}  placeholders={n}\n")
            f.write("\n")

        if missing_files:
            f.write("---- MISSING STATIC FILE REFERENCES ----\n")
            for src, kind, raw, res in missing_files[:500]:
                f.write(f"{src.relative_to(root)} :: {kind}='{raw}' -> MISSING {res}\n")
            if len(missing_files) > 500:
                f.write(f"... ({len(missing_files)-500} more)\n")
            f.write("\n")

        if outside_refs:
            f.write("---- FILE REFERENCES THAT ESCAPE ROOT ----\n")
            for src, kind, raw, res in outside_refs[:200]:
                f.write(f"{src.relative_to(root)} :: {kind}='{raw}' -> OUTSIDE {res}\n")
            if len(outside_refs) > 200:
                f.write(f"... ({len(outside_refs)-200} more)\n")
            f.write("\n")

        if dead_button_pages:
            f.write("---- HEURISTIC DEAD BUTTON PAGES ----\n")
            for fp in sorted(set(dead_button_pages)):
                f.write(str(fp.relative_to(root)) + "\n")
            f.write("\n")

        if orphans:
            f.write("---- ORPHAN HTML FILES ----\n")
            for fp in sorted(set(orphans)):
                f.write(str(fp.relative_to(root)) + "\n")
            f.write("\n")

        f.write("---- PER-FILE SUMMARY ----\n")
        for info in pages:
            rel = info["file"].relative_to(root)
            f.write(
                f"{rel} | href={info['href_count']} src={info['src_count']} action={info['action_count']} "
                f"forms={info['form_opens']}/{info['form_closes']} buttons={info['buttons']} submits={info['submits']} "
                f"placeholders={len(info['placeholders'])}\n"
            )

    print(f"[OK] wrote {out}")

if __name__ == "__main__":
    main()
