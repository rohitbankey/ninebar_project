from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Config:
    raw: dict
    root: Path

    @property
    def tz(self) -> str:
        return self.raw["timezone"]

    @property
    def window(self) -> tuple[date, date]:
        w = self.raw["window"]
        return _d(w["start"]), _d(w["end"])

    @property
    def areas(self) -> dict:
        return self.raw["areas"]

    def path(self, key: str) -> Path:
        p = Path(os.environ.get(f"NINEBAR_{key.upper()}", self.raw["paths"][key]))
        return p if p.is_absolute() else self.root / p

    @property
    def fortnight_anchor(self) -> date:
        return _d(self.raw["cadences"]["fortnightly"]["anchor_date"])

    @property
    def fortnight_days(self) -> int:
        return int(self.raw["cadences"]["fortnightly"]["length_days"])

    @property
    def mask_salt(self) -> str:
        return os.environ.get("NINEBAR_MASK_SALT", self.raw["masking"]["default_salt"])


def _d(v) -> date:
    return v if isinstance(v, date) else date.fromisoformat(str(v))


def load(path: Path | None = None) -> Config:
    path = path or Path(os.environ.get("NINEBAR_CONFIG", ROOT / "config" / "pipeline.yaml"))
    with open(path) as f:
        raw = yaml.safe_load(f)
    return Config(raw=raw, root=ROOT)
