"""
PRISM POC: Willamette Valley AVA → Railway PostgreSQL
======================================================
Designed to run as a one-shot Railway service (deploy → runs → exits).
DATABASE_URL is injected automatically by Railway when a Postgres
service exists in the same project.

All config is via environment variables (set in Railway dashboard):
  DATABASE_URL  — injected by Railway automatically
  AVA_FILE      — default: oregon_avas.geojson
  NAME_COL      — default: Name
  START         — default: 2020-01
  END           — default: 2022-12
  RESOLUTION    — default: 4km
"""

import io
import os
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import requests
from rasterstats import zonal_stats
from sqlalchemy import Column, Float, Integer, String, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session
from tqdm import tqdm

# ── Config from env (with defaults) ──────────────────────────────────────────
SHAPEFILE  = os.environ.get("AVA_FILE",    "oregon_avas.geojson")
NAME_COL   = os.environ.get("NAME_COL",   "Name")
START      = os.environ.get("START",      "2020-01")
END        = os.environ.get("END",        "2022-12")
RESOLUTION = os.environ.get("RESOLUTION", "4km")
CACHE_DIR  = Path(os.environ.get("CACHE_DIR", "/cache"))

VARIABLES  = ["ppt", "tmin", "tmax", "tmean", "tdmean", "vpdmin", "vpdmax"]
WS_BASE    = f"https://services.nacse.org/prism/data/get/us/{RESOLUTION}"
SCHEMA     = "prism"
TABLE      = "ava_climate_poc"
SLEEP_SEC  = 2

# ── ORM ───────────────────────────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass

class AvaClimatePoc(Base):
    __tablename__ = TABLE
    __table_args__ = {"schema": SCHEMA}

    id          = Column(Integer, primary_key=True, autoincrement=True)
    ava_name    = Column(String(255), nullable=False, index=True)
    year        = Column(Integer, nullable=False)
    month       = Column(Integer, nullable=False)

    ppt_mean    = Column(Float); ppt_min    = Column(Float); ppt_max    = Column(Float)
    tmin_mean   = Column(Float); tmin_min   = Column(Float); tmin_max   = Column(Float)
    tmax_mean   = Column(Float); tmax_min   = Column(Float); tmax_max   = Column(Float)
    tmean_mean  = Column(Float); tmean_min  = Column(Float); tmean_max  = Column(Float)
    tdmean_mean = Column(Float); tdmean_min = Column(Float); tdmean_max = Column(Float)
    vpdmin_mean = Column(Float); vpdmin_min = Column(Float); vpdmin_max = Column(Float)
    vpdmax_mean = Column(Float); vpdmax_min = Column(Float); vpdmax_max = Column(Float)

# ── Helpers ───────────────────────────────────────────────────────────────────
def month_range(start: str, end: str):
    sy, sm = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]),   int(end[5:7])
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m, y = 1, y + 1

def download_tif(var: str, year: int, month: int) -> Path | None:
    tif = CACHE_DIR / var / f"prism_{var}_us_{RESOLUTION}_{year}{month:02d}.tif"
    if tif.exists():
        return tif
    tif.parent.mkdir(parents=True, exist_ok=True)
    url = f"{WS_BASE}/{var}/{year}{month:02d}"
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            tif_names = [n for n in zf.namelist() if n.endswith(".tif")]
            if not tif_names:
                return None
            with zf.open(tif_names[0]) as src, open(tif, "wb") as dst:
                dst.write(src.read())
    except Exception as e:
        print(f"  [WARN] {var} {year}-{month:02d}: {e}")
        return None
    time.sleep(SLEEP_SEC)
    return tif

def get_stats(tif: Path, gdf: gpd.GeoDataFrame) -> dict:
    try:
        results = zonal_stats(gdf, str(tif), stats=["mean", "min", "max"],
                              nodata=np.nan, all_touched=True)
        s = results[0]
        return {k: (None if v is None or np.isnan(float(v)) else round(float(v), 4))
                for k, v in s.items()}
    except Exception as e:
        print(f"  [WARN] zonal_stats: {e}")
        return {"mean": None, "min": None, "max": None}

def get_engine():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise EnvironmentError(
            "DATABASE_URL is not set. "
            "Railway injects this automatically when a Postgres service "
            "exists in the same project."
        )
    return create_engine(
        url.replace("postgres://", "postgresql://", 1),
        pool_pre_ping=True
    )

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"PRISM POC  |  AVA file: {SHAPEFILE}  |  {START} → {END}")

    gdf = gpd.read_file(SHAPEFILE).to_crs("EPSG:4326")
    ava_name = gdf[NAME_COL].iloc[0]
    print(f"AVA: {ava_name}\n")

    engine = get_engine()

    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        conn.commit()

    Base.metadata.create_all(engine)

    with engine.connect() as conn:
        try:
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_poc_ava_year_month "
                f"ON {SCHEMA}.{TABLE} (ava_name, year, month)"
            ))
            conn.execute(text(
                f"COMMENT ON TABLE {SCHEMA}.{TABLE} IS "
                f"'POC: PRISM 4km monthly climate for {ava_name} ({START}–{END}). "
                f"Spatial mean/min/max per AVA polygon. Source: prism.oregonstate.edu'"
            ))
        except Exception:
            pass
        conn.commit()

    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHEMA}.{TABLE} RESTART IDENTITY"))
        conn.commit()
    print("Table truncated — loading fresh.\n")

    months = list(month_range(START, END))
    total  = len(months) * len(VARIABLES)
    print(f"{len(months)} months × {len(VARIABLES)} variables = {total} grid downloads")
    print(f"Estimated time: ~{round(total * SLEEP_SEC / 60, 0):.0f} minutes\n")

    rows = []
    for year, month in tqdm(months, desc="Progress"):
        kwargs = dict(ava_name=ava_name, year=year, month=month)
        for var in VARIABLES:
            tif = download_tif(var, year, month)
            s   = get_stats(tif, gdf) if tif else {"mean": None, "min": None, "max": None}
            kwargs[f"{var}_mean"] = s["mean"]
            kwargs[f"{var}_min"]  = s["min"]
            kwargs[f"{var}_max"]  = s["max"]
        rows.append(AvaClimatePoc(**kwargs))

    with Session(engine) as session:
        session.add_all(rows)
        session.commit()

    print(f"\n✓ Done — {len(rows)} rows written to {SCHEMA}.{TABLE}")

if __name__ == "__main__":
    main()
