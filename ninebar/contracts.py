"""Contract registry and drift detection.

Contracts are seeded from contracts/*.yaml into ops.contract_registry. After
that the registry table is the source of truth, because versions can also be
added at runtime (additive drift) or by a migration (breaking drift).

Resolution of an arrival against the registry:
  * try every registered version, newest first
  * exact match            -> use that version
  * match plus new columns -> additive: register <major>.<minor+1>, carry on
  * nothing matches        -> breaking: the arrival is held, and the problems
                              (missing column, probable rename, type change)
                              are written to the manifest
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import yaml

SAMPLE_LINES = 5000
# The gate is looking for a column whose *type* changed, which shows up as most values
# failing to cast. A few bad values are row-level junk: that is the quarantine's job,
# and the per-arrival quality threshold decides whether the file is usable.
CAST_OK_RATIO = 0.5


@dataclass
class Version:
    dataset: str
    version: str
    spec: dict

    @property
    def columns(self) -> list[dict]:
        return self.spec["columns"]

    @property
    def rename(self) -> dict:
        return self.spec.get("rename", {})

    def source_name(self, canonical: str) -> str:
        inv = {v: k for k, v in self.rename.items()}
        return inv.get(canonical, canonical)

    @property
    def key(self) -> tuple[int, int]:
        major, minor = self.version.split(".")
        return int(major), int(minor)


@dataclass
class Observed:
    header: list[str] | None
    ncols: int
    samples: list[list[str]]  # column-wise sample values


@dataclass
class Resolution:
    kind: str                       # match | additive | breaking
    version: Version | None
    positions: dict[str, int] = field(default_factory=dict)   # canonical column -> 0-based field index
    new_columns: list[dict] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    has_header: bool = False


# ---------------------------------------------------------------- registry

def load_yaml(contracts_dir: Path) -> dict[str, dict]:
    out = {}
    for p in sorted(contracts_dir.glob("*.yaml")):
        doc = yaml.safe_load(p.read_text())
        out[doc["dataset"]] = doc
    return out


def seed(con, contracts_dir: Path) -> None:
    for ds, doc in load_yaml(contracts_dir).items():
        for v in doc["versions"]:
            spec = {"columns": v["columns"], "rename": v.get("rename", {}),
                    "format": doc["format"], "policy": doc["policy"],
                    "quarantine_threshold": doc.get("quarantine_threshold", 0.01),
                    "owner": doc.get("owner")}
            con.execute(
                "INSERT INTO ops.contract_registry (dataset, version, spec, source) VALUES (?,?,?, 'seed') "
                "ON CONFLICT DO NOTHING", [ds, v["version"], json.dumps(spec)])


def versions(con, dataset: str) -> list[Version]:
    rows = con.execute("SELECT version, spec FROM ops.contract_registry WHERE dataset = ?", [dataset]).fetchall()
    vs = [Version(dataset, v, json.loads(s)) for v, s in rows]
    return sorted(vs, key=lambda v: v.key, reverse=True)


def latest(con, dataset: str) -> Version:
    return versions(con, dataset)[0]


def register(con, dataset: str, version: str, spec: dict, source: str) -> Version:
    con.execute("INSERT INTO ops.contract_registry (dataset, version, spec, source) VALUES (?,?,?,?)",
                [dataset, version, json.dumps(spec), source])
    return Version(dataset, version, spec)


# ---------------------------------------------------------------- observing a file

def _numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        return False


def observe(path: Path, delimiter: str) -> Observed:
    lines: list[str] = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if i >= SAMPLE_LINES:
                break
            lines.append(line.rstrip("\r\n"))
    if not lines:
        return Observed(None, 0, [])
    first = lines[0].split(delimiter)
    header = None
    if all(x.strip() and not _numeric(x.strip()) for x in first):
        header = [x.strip() for x in first]
        lines = lines[1:]
    rows = [ln.split(delimiter) for ln in lines if ln]
    ncols = len(header) if header else (Counter(len(r) for r in rows).most_common(1)[0][0] if rows else 0)
    cols = [[r[i] for r in rows if len(r) == ncols and r[i] != ""] for i in range(ncols)]
    return Observed(header, ncols, cols)


def castable(values: list[str], typ: str) -> float:
    if not values:
        return 1.0
    ok = 0
    for v in values:
        try:
            if typ in ("INTEGER", "BIGINT"):
                int(v)
            elif typ == "DOUBLE":
                float(v)
            elif typ == "DATE":
                date.fromisoformat(v)
            ok += 1
        except ValueError:
            pass
    return ok / len(values)


def infer_type(values: list[str]) -> str:
    if values and castable(values, "BIGINT") == 1.0:
        return "BIGINT"
    if values and castable(values, "DOUBLE") == 1.0:
        return "DOUBLE"
    return "VARCHAR"


# ---------------------------------------------------------------- resolution

def _try_version(v: Version, obs: Observed) -> Resolution:
    res = Resolution(kind="match", version=v, has_header=obs.header is not None)
    if obs.header:
        index = {n: i for i, n in enumerate(obs.header)}
    else:
        index = {v.source_name(c["name"]): i for i, c in enumerate(v.columns) if i < obs.ncols}
    used = set()
    for c in v.columns:
        src = v.source_name(c["name"])
        if src not in index:
            res.problems.append(f"missing column '{src}' (canonical '{c['name']}')")
            continue
        pos = index[src]
        used.add(pos)
        res.positions[c["name"]] = pos
        ratio = castable(obs.samples[pos], c["type"])
        if ratio < CAST_OK_RATIO:
            res.problems.append(f"type change on '{src}': only {ratio:.0%} of sampled values cast to {c['type']}")
    for i in range(obs.ncols):
        if i in used:
            continue
        name = obs.header[i] if obs.header else f"extra_col_{i + 1}"
        res.new_columns.append({"name": name, "type": infer_type(obs.samples[i]), "nullable": True, "position": i})
    if res.problems:
        res.kind = "breaking"
        # a missing column plus an unexpected one of the same type at the same position is almost always a rename
        for c in v.columns:
            src = v.source_name(c["name"])
            if obs.header and src not in obs.header:
                pos = [i for i, x in enumerate(v.columns) if x["name"] == c["name"]][0]
                cand = [n for n in res.new_columns if n["position"] == pos]
                if cand:
                    res.problems.append(f"probable rename: '{src}' -> '{cand[0]['name']}'")
    elif res.new_columns:
        res.kind = "additive"
    return res


def resolve(con, dataset: str, obs: Observed) -> Resolution:
    vs = versions(con, dataset)
    first: Resolution | None = None
    for v in vs:
        r = _try_version(v, obs)
        first = first or r
        if r.kind == "match":
            return r
        if r.kind == "additive":
            # prefer an exact match with an older version over adding a new one,
            # but an additive match beats a breaking one
            exact = [x for x in (_try_version(o, obs) for o in vs) if x.kind == "match"]
            return exact[0] if exact else r
    assert first is not None
    first.kind = "breaking"
    return first


def register_additive(con, res: Resolution) -> Version:
    base = res.version
    top = latest(con, base.dataset)
    new_version = f"{top.key[0]}.{top.key[1] + 1}"
    spec = json.loads(json.dumps(base.spec))
    for n in res.new_columns:
        spec["columns"].append({"name": n["name"], "type": n["type"], "nullable": True})
        res.positions[n["name"]] = n["position"]
    v = register(con, base.dataset, new_version, spec, "auto-additive")
    res.version = v
    res.new_columns = []
    res.kind = "match"
    return v


def apply_migration(con, migration_path: Path) -> tuple[str, str, int]:
    """Deliberately accept a breaking change. Returns (migration_id, new_version, released_count)."""
    m = yaml.safe_load(migration_path.read_text())
    mid, ds = m["id"], m["dataset"]
    if con.execute("SELECT 1 FROM ops.applied_migrations WHERE migration_id = ?", [mid]).fetchone():
        raise ValueError(f"migration {mid} already applied")
    base = latest(con, ds)
    spec = json.loads(json.dumps(base.spec))
    rename = dict(spec.get("rename", {}))
    rename.update(m["change"].get("rename", {}))
    spec["rename"] = rename
    for col, typ in m["change"].get("retype", {}).items():
        for c in spec["columns"]:
            if c["name"] == col:
                c["type"] = typ
    new_version = f"{base.key[0] + 1}.0"
    register(con, ds, new_version, spec, f"migration:{mid}")
    released = con.execute(
        "UPDATE ops.landing_manifest SET status = 'PENDING', status_reason = 'released by migration ' || ? "
        "WHERE dataset = ? AND status = 'HELD_SCHEMA' RETURNING arrival_id", [mid, ds]).fetchall()
    con.execute("INSERT INTO ops.applied_migrations VALUES (?,?,?, now(), ?)", [mid, ds, new_version, len(released)])
    return mid, new_version, len(released)
