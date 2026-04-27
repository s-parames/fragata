#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pymysql


QUEUE_TO_DEPARTMENT = {
    1: "general",
    3: "sistemas",
    5: "aplicaciones",
    6: "comunicaciones",
    30: "bigdata",
}

TRANSACTION_TYPES = ("Create", "Correspond", "Comment")
OLD_MESSAGE_KEYWORDS = ("escribiu:\n", "escribio:\n", "wrote:\n", "de: ", "from: ")
CREATE_MESSAGE_KEYWORDS = (
    "helpdesk_aplicaciones@cesga.es\n",
    "helpdesk_sistemas@cesga.es\n",
    "* descripcion:",
)
UTC = timezone.utc


def env_required(name: str) -> str:
    value = str(os.getenv(name, "")).strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def parse_db_datetime(raw: str) -> datetime:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("empty datetime")
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def to_utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def decode_blob(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def clean_message(content: str, ticket_id: int) -> str:
    message = html.unescape(str(content or ""))
    message = message.replace("\r\n", "\n").replace("\r", "\n")
    if not message.strip():
        return ""

    lowered = message.lower()
    for keyword in OLD_MESSAGE_KEYWORDS:
        idx = lowered.find(keyword)
        if idx > 0:
            message = message[:idx]
            lowered = message.lower()

    for marker in CREATE_MESSAGE_KEYWORDS:
        idx = lowered.find(marker)
        if idx >= 0:
            tail = message[idx + len(marker) :].lstrip("\n ")
            if tail:
                message = tail
                lowered = message.lower()

    message = message.replace(
        f"\n<URL: https://rt.lan.cesga.es/Ticket/Display.html?id={ticket_id} >\n\n",
        "",
    )
    message = re.sub(r"(?m)^\s*[-_]{3,}\s*$", "", message)
    message = re.sub(r"\n{3,}", "\n\n", message)
    return message.strip()


def transaction_role(tx_type: str, creator_id: int, users_email: Dict[int, str]) -> str:
    if str(tx_type or "").lower() == "comment":
        return "comment"
    email = users_email.get(int(creator_id or 0), "").lower()
    return "assistant" if "cesga" in email else "user"


def fetch_users_email(conn: pymysql.connections.Connection) -> Dict[int, str]:
    out: Dict[int, str] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, EmailAddress FROM Users")
        for row in cur.fetchall() or []:
            try:
                out[int(row["id"])] = decode_blob(row.get("EmailAddress"))
            except Exception:
                continue
    return out


def fetch_tickets(
    conn: pymysql.connections.Connection,
    queue_id: int,
    since_dt: datetime,
    until_dt: datetime,
) -> List[Dict[str, Any]]:
    sql = (
        "SELECT id, LastUpdated, Status, Subject "
        "FROM Tickets "
        "WHERE Queue=%s AND Status='resolved' "
        "  AND LastUpdated > %s AND LastUpdated <= %s "
        "ORDER BY LastUpdated ASC, id ASC"
    )
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                int(queue_id),
                since_dt.strftime("%Y-%m-%d %H:%M:%S"),
                until_dt.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        return list(cur.fetchall() or [])


def fetch_transactions(
    conn: pymysql.connections.Connection,
    ticket_id: int,
) -> List[Dict[str, Any]]:
    sql = (
        "SELECT id, Type, Creator "
        "FROM Transactions "
        "WHERE ObjectType='RT::Ticket' "
        "  AND ObjectId=%s "
        "  AND Type IN ('Create','Correspond','Comment') "
        "ORDER BY Created ASC, id ASC"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (int(ticket_id),))
        return list(cur.fetchall() or [])


def fetch_attachment_content(
    conn: pymysql.connections.Connection,
    transaction_id: int,
) -> str:
    sql = "SELECT Content FROM Attachments WHERE TransactionId=%s ORDER BY id ASC"
    with conn.cursor() as cur:
        cur.execute(sql, (int(transaction_id),))
        rows = cur.fetchall() or []
    for row in rows:
        text = decode_blob(row.get("Content"))
        if text:
            return text
    return ""


def build_messages(
    conn: pymysql.connections.Connection,
    ticket_id: int,
    users_email: Dict[int, str],
) -> List[Dict[str, str]]:
    txs = fetch_transactions(conn, ticket_id)
    messages: List[Dict[str, str]] = []
    for tx in txs:
        tx_id = int(tx.get("id") or 0)
        if tx_id <= 0:
            continue
        tx_type = decode_blob(tx.get("Type"))
        creator_id = int(tx.get("Creator") or 0)
        raw = fetch_attachment_content(conn, tx_id)
        cleaned = clean_message(raw, ticket_id=ticket_id)
        if not cleaned:
            continue
        role = transaction_role(tx_type, creator_id, users_email)
        messages.append({"role": role, "content": cleaned})

    if messages and messages[0].get("role") != "user":
        messages[0]["role"] = "user"
    return messages


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Download resolved RT tickets by window and emit per-department JSONL plus summary."
    )
    ap.add_argument("--since", required=True, help="Lower bound (exclusive) as UTC datetime")
    ap.add_argument("--until", required=True, help="Upper bound (inclusive) as UTC datetime")
    ap.add_argument("--out-dir", required=True, help="Directory for per-department JSONL outputs")
    ap.add_argument("--summary-out", required=True, help="Summary JSON path for pipeline integration")
    ap.add_argument("--output-mode", default="pipeline", choices=("pipeline", "verbose"))
    ap.add_argument("--output-prefix", default="resolved_tickets")
    ap.add_argument("--run-stamp", default="")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    since_dt = parse_db_datetime(args.since)
    until_dt = parse_db_datetime(args.until)
    if since_dt >= until_dt:
        raise SystemExit(f"Invalid window: since ({args.since}) must be < until ({args.until})")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_out = Path(args.summary_out)
    summary_out.parent.mkdir(parents=True, exist_ok=True)

    run_stamp = str(args.run_stamp or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")).strip()
    output_prefix = str(args.output_prefix or "resolved_tickets").strip() or "resolved_tickets"

    conn = pymysql.connect(
        host=env_required("DAILY_DB_HOST"),
        port=int(os.getenv("DAILY_DB_PORT", "3306")),
        user=env_required("DAILY_DB_USER"),
        password=env_required("DAILY_DB_PASSWORD"),
        database=env_required("DAILY_DB_NAME"),
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=15,
        read_timeout=300,
        write_timeout=300,
        use_unicode=False,
    )

    counts_by_department: Dict[str, int] = {}
    files: List[Dict[str, Any]] = []
    total_rows = 0
    max_last_updated: Optional[datetime] = None

    try:
        users_email = fetch_users_email(conn)
        for queue_id, department in QUEUE_TO_DEPARTMENT.items():
            tickets = fetch_tickets(conn, queue_id, since_dt=since_dt, until_dt=until_dt)
            if not tickets:
                continue

            out_path = out_dir / f"{output_prefix}_{department}_{run_stamp}.jsonl"
            row_count = 0
            with out_path.open("w", encoding="utf-8") as fh:
                for ticket in tickets:
                    ticket_id = int(ticket.get("id") or 0)
                    if ticket_id <= 0:
                        continue
                    messages = build_messages(conn, ticket_id=ticket_id, users_email=users_email)
                    if not messages:
                        continue

                    last_updated_raw = decode_blob(ticket.get("LastUpdated"))
                    if not last_updated_raw:
                        continue
                    last_updated_dt = parse_db_datetime(last_updated_raw)
                    if max_last_updated is None or last_updated_dt > max_last_updated:
                        max_last_updated = last_updated_dt

                    row = {
                        "ticket_id": ticket_id,
                        "link": f"https://rt.lan.cesga.es/Ticket/Display.html?id={ticket_id}",
                        "department": department,
                        "lastUpdated": last_updated_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "status": decode_blob(ticket.get("Status")) or "resolved",
                        "subject": html.unescape(decode_blob(ticket.get("Subject"))).strip(),
                        "messages": messages,
                    }
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                    row_count += 1
                    total_rows += 1

            if row_count <= 0:
                out_path.unlink(missing_ok=True)
                continue

            counts_by_department[department] = row_count
            files.append(
                {
                    "department": department,
                    "path": str(out_path.resolve()),
                    "rows": row_count,
                }
            )
    finally:
        conn.close()

    summary = {
        "window": {
            "since": since_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "until": until_dt.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "total_rows": total_rows,
        "counts_by_department": counts_by_department,
        "files": files,
        "max_row_last_updated_utc": to_utc_iso(max_last_updated) if max_last_updated else None,
    }
    summary_out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.output_mode == "verbose":
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(
            f"[ticketDownloader] rows={total_rows} departments={len(counts_by_department)} "
            f"summary={summary_out}"
        )


if __name__ == "__main__":
    main()
