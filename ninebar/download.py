"""Fetch the Telecom Italia files from Harvard Dataverse.

The datasets sit behind guestbook 96. A plain GET is refused with a 400, but a
POST to the access endpoint (with an optional guestbook response body) returns a
short-lived signed URL, and that URL can then be fetched with a normal GET.

Files go into data/raw_cache/. Nothing in raw_cache is treated as "landed" -
landing is a separate, explicit step (see landing.py) so that a half-finished
download never becomes an arrival.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

DATAVERSE = "https://dataverse.harvard.edu"
DATASETS = {
    "MI": "doi:10.7910/DVN/EGZHFV",
    "TN": "doi:10.7910/DVN/QLCABU",
    "MI_GRID": "doi:10.7910/DVN/QJWLFU",
    "TN_GRID": "doi:10.7910/DVN/FZRVSX",
}
UA = "ninebar-pipeline/0.1 (+curl-compatible)"


def _get_json(url: str, data: bytes | None = None) -> dict:
    req = urllib.request.Request(url, data=data, method="POST" if data is not None else "GET")
    req.add_header("User-Agent", UA)  # Dataverse 403s the default python-urllib agent
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def list_files(doi: str) -> dict[str, int]:
    meta = _get_json(f"{DATAVERSE}/api/datasets/:persistentId/?persistentId={doi}")
    return {f["label"]: f["dataFile"]["id"] for f in meta["data"]["latestVersion"]["files"]}


def signed_url(file_id: int) -> str:
    # Guestbook 96 only asks for an email and the server currently accepts an empty
    # response. If that ever changes, set DV_EMAIL and it is sent along.
    body: dict = {}
    if os.environ.get("DV_EMAIL"):
        body = {"guestbookResponse": {"email": os.environ["DV_EMAIL"], "name": os.environ.get("DV_NAME", "")}}
    resp = _get_json(f"{DATAVERSE}/api/access/datafile/{file_id}", json.dumps(body).encode())
    return resp["data"]["signedUrl"]


def fetch(file_id: int, dest: Path, retries: int = 4) -> Path:
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            url = signed_url(file_id)  # links expire, so ask for a fresh one each attempt
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=120) as r, open(tmp, "wb") as f:
                shutil.copyfileobj(r, f, length=1 << 20)
            tmp.rename(dest)  # rename is the commit point
            return dest
        except Exception as e:  # noqa: BLE001 - network errors come in many shapes
            print(f"  retry {attempt}/{retries} for {dest.name}: {e}", file=sys.stderr)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"giving up on {dest.name}")


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def download(cache: Path, start: date, end: date, areas=("MI", "TN"), workers: int = 4) -> None:
    jobs: list[tuple[int, Path]] = []
    for area in areas:
        files = list_files(DATASETS[area])
        for d in daterange(start, end):
            name = f"sms-call-internet-{area.lower()}-{d.isoformat()}.txt"
            if name not in files:
                print(f"  {name} not published on Dataverse, skipping")
                continue
            jobs.append((files[name], cache / area / name))
        grid_key = f"{area}_GRID"
        for label, fid in list_files(DATASETS[grid_key]).items():
            jobs.append((fid, cache / "grid" / label))

    todo = [j for j in jobs if not j[1].exists()]
    print(f"{len(jobs)} files in window, {len(todo)} still to fetch")
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(fetch, fid, p): p for fid, p in todo}
        for fut in as_completed(futs):
            p = fut.result()
            print(f"  got {p.relative_to(cache)} ({p.stat().st_size / 1e6:.0f} MB)")
