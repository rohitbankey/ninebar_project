"""Immutable landing zone (the bronze layer).

An arrival is a file plus a manifest row. Files are written under a path that
includes the arrival timestamp and content hash, then made read-only, so a
re-delivery of the same logical file (same dataset/area/date) sits next to the
earlier one instead of replacing it. Nothing in landing is ever modified or
deleted by the pipeline - replays read from here.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
from datetime import date, datetime, timezone
from pathlib import Path


def file_digest(path: Path) -> tuple[str, int, int]:
    h = hashlib.sha256()
    lines = 0
    size = 0
    with open(path, "rb") as f:
        while chunk := f.read(1 << 22):
            h.update(chunk)
            lines += chunk.count(b"\n")
            size += len(chunk)
    return h.hexdigest(), size, lines


def land(con, landing_root: Path, src: Path, dataset: str, area: str | None,
         file_date: date, origin: str) -> tuple[int, str]:
    """Register one arrival. Returns (arrival_id, status)."""
    sha, size, lines = file_digest(src)
    now = datetime.now(timezone.utc)

    dup = con.execute(
        """SELECT arrival_id FROM ops.landing_manifest
           WHERE dataset = ? AND coalesce(area, '') = coalesce(?, '') AND file_date = ? AND sha256 = ?
             AND status <> 'DUPLICATE'""",
        [dataset, area, file_date, sha],
    ).fetchone()

    part = landing_root / dataset / f"area={area or 'ALL'}" / f"file_date={file_date.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    dest = part / f"{now.strftime('%Y%m%dT%H%M%S%fZ')}_{sha[:12]}{src.suffix}"
    try:
        os.link(src, dest)  # cheap when cache and landing share a filesystem
    except OSError:
        shutil.copy2(src, dest)
    os.chmod(dest, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    arrival_id = con.execute("SELECT coalesce(max(arrival_id), 0) + 1 FROM ops.landing_manifest").fetchone()[0]
    status = "DUPLICATE" if dup else "PENDING"
    reason = f"same content as arrival {dup[0]}" if dup else None
    con.execute(
        "INSERT INTO ops.landing_manifest VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL)",
        [arrival_id, dataset, area, file_date, str(dest), sha, size, lines, now, origin, status, reason],
    )
    sidecar = {
        "arrival_id": arrival_id, "dataset": dataset, "area": area, "file_date": file_date.isoformat(),
        "source_file": src.name, "origin": origin, "arrived_at": now.isoformat(),
        "sha256": sha, "bytes": size, "row_count": lines, "status_at_arrival": status,
    }
    dest.with_name(dest.name + ".manifest.json").write_text(json.dumps(sidecar, indent=2))
    return arrival_id, status
