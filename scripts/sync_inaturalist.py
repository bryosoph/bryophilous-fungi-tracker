#!/usr/bin/env python3
"""
Sync bryophilous fungi observations from iNaturalist API.
Fetches all observations for the target taxa and saves to CSV and SQLite.
"""

import os
import csv
import time
import sqlite3
import logging
import requests
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Target taxa ────────────────────────────────────────────────────────────────
GENUS_TAXA = [
    "Bryoscyphus", "Bryochiton", "Chromocyphella", "Lamprospora",
    "Lizonia", "Octosporella", "Epibryon", "Bryocentria", "Rimbachia",
    "Eocronartium", "Luteodiscus", "Bryonectria", "Loreleia", "Helotium",
    "Bryosphaeria", "Potridiscus", "Gloeopeziza", "Coniochaeta",
    "Bryobroma", "Pithyella", "Bryopistillaria",
]

SPECIES_TAXA = [
    "Pezoloma marchantiae", "Bryorutstroemia fulva", "Cistella polytrichi",
    "Neottiella ricciae", "Muscinupta laevis", "Mniaecia jungermanniae",
    "Pleostigma jungermannicola", "Bryoscyphus dicrani",
    "Bryostroma trichostomi", "Arrhenia retiruga",
    "Bryochiton microscopicus", "Neottiella rutilans",
    "Chromocyphella muscicola", "Lamprospora tortulae-ruralis",
    "Bryoscyphus turbinatus", "Lamprospora miniata",
    "Lamprospora hispanica", "Mniaecia nivea", "Dactylospora scapanaria",
    "Durella polytrichina", "Octospora coccinea", "Rimbachia bryophila",
    "Eocronartium muscicola", "Rimbachia arachnoidea",
    "Bryocentria hypothallina", "Octospora axillaris",
    "Octospora excipulata", "Luteodiscus hemiamyloideus",
    "Epibryon metzgeriae", "Epibryon casaresii", "Bryoglossum gracile",
    "Octospora affinis", "Octospora rustica", "Lizonia baldinii",
    "Rimbachia neckerae", "Octospora humosa", "Octospora musci-muralis",
    "Epibryon plagiochilae", "Lamprospora wrightii", "Octospora gemmicola",
    "Helotium schimperi", "Mniaecia gloeocapsae",
    "Bryocentria metzgeriae", "Bryosphaeria bryophila",
    "Octospora itzerottii", "Octospora lilacina", "Epibryon bryophilum",
    "Bryocentria octosporelloides", "Bryoscyphus conocephali",
    "Epibryon muscicola", "Octosporella perforata",
    "Octospora gyalectoides", "Bryochiton macrosporus",
    "Bryochiton monascus", "Octospora fissidentis", "Epibryon turfosorum",
    "Octospora orthotrichi", "Coniochaetaceae",
    "Bryobroma microcarpum", "Bryobroma velenovskyi",
    "Bryobroma microcarpum var. racomitrii", "Hilberina sphagnorum",
    "Epibryon diaphanum", "Epibryon hepaticicola", "Epibryon dicrani",
    "Epibryon interlamellare", "Octospora leucoloma",
    "Hyphodiscus delitescens", "Pithyella chalaudii",
    "Bryocentria brongniartii", "Hymenoscyphus vasaensis",
    "Rickenella swartzii",
]

# ── iNaturalist API ────────────────────────────────────────────────────────────
INAT_BASE = "https://api.inaturalist.org/v1"
PAGE_SIZE = 200
REQUEST_DELAY = 1.0   # seconds between requests (be polite)

CSV_FIELDS = [
    "id", "uuid", "quality_grade", "observed_on", "observed_time_zone",
    "created_at", "updated_at",
    "taxon_id", "taxon_name", "taxon_rank", "taxon_common_name",
    "taxon_ancestry",
    "latitude", "longitude", "positional_accuracy",
    "place_guess", "place_ids",
    "obscured", "geoprivacy",
    "user_id", "user_login", "user_name",
    "num_identification_agreements", "num_identification_disagreements",
    "identifications_most_agree", "species_guess",
    "description",
    "tag_list",
    "url",
    "image_url",
    "sound_url",
    "license_code",
    "captive_cultivated",
    "out_of_range",
    "native",
    "introduced",
    "endemic",
    "threatened",
]


def resolve_taxon_id(name: str) -> int | None:
    """Return iNaturalist taxon_id for a name (genus or species)."""
    url = f"{INAT_BASE}/taxa"
    params = {"q": name, "per_page": 5, "is_active": "true"}
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        results = r.json().get("results", [])
        for t in results:
            if t["name"].lower() == name.lower():
                return t["id"]
        if results:
            return results[0]["id"]
    except Exception as e:
        log.warning(f"Taxon lookup failed for '{name}': {e}")
    return None


def fetch_observations_for_taxon(taxon_id: int, taxon_name: str) -> list[dict]:
    """Page through all observations for a taxon_id."""
    all_obs = []
    page = 1
    while True:
        url = f"{INAT_BASE}/observations"
        params = {
            "taxon_id": taxon_id,
            "per_page": PAGE_SIZE,
            "page": page,
            "order": "desc",
            "order_by": "created_at",
            "include_new_projects": "true",
        }
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error(f"Request error for taxon {taxon_name} page {page}: {e}")
            break

        results = data.get("results", [])
        total = data.get("total_results", 0)
        log.info(f"  {taxon_name}: page {page} — {len(results)} obs (total={total})")

        for obs in results:
            all_obs.append(parse_observation(obs))

        if len(all_obs) >= total or not results:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    return all_obs


def parse_observation(obs: dict) -> dict:
    """Flatten an iNaturalist observation JSON into a row dict."""
    taxon = obs.get("taxon") or {}
    user = obs.get("user") or {}
    loc = obs.get("location") or ""
    lat, lon = ("", "")
    if loc and "," in loc:
        parts = loc.split(",", 1)
        lat, lon = parts[0].strip(), parts[1].strip()

    photos = obs.get("photos") or []
    sounds = obs.get("sounds") or []
    place_ids = obs.get("place_ids") or []

    common = ""
    if taxon.get("preferred_common_name"):
        common = taxon["preferred_common_name"]
    elif taxon.get("english_common_name"):
        common = taxon["english_common_name"]

    conservation = taxon.get("conservation_status") or {}

    return {
        "id": obs.get("id", ""),
        "uuid": obs.get("uuid", ""),
        "quality_grade": obs.get("quality_grade", ""),
        "observed_on": obs.get("observed_on", ""),
        "observed_time_zone": obs.get("observed_time_zone", ""),
        "created_at": obs.get("created_at", ""),
        "updated_at": obs.get("updated_at", ""),
        "taxon_id": taxon.get("id", ""),
        "taxon_name": taxon.get("name", ""),
        "taxon_rank": taxon.get("rank", ""),
        "taxon_common_name": common,
        "taxon_ancestry": taxon.get("ancestry", ""),
        "latitude": lat,
        "longitude": lon,
        "positional_accuracy": obs.get("positional_accuracy", ""),
        "place_guess": obs.get("place_guess", ""),
        "place_ids": "|".join(str(p) for p in place_ids),
        "obscured": obs.get("obscured", ""),
        "geoprivacy": obs.get("geoprivacy", ""),
        "user_id": user.get("id", ""),
        "user_login": user.get("login", ""),
        "user_name": user.get("name", ""),
        "num_identification_agreements": obs.get("num_identification_agreements", ""),
        "num_identification_disagreements": obs.get("num_identification_disagreements", ""),
        "identifications_most_agree": obs.get("identifications_most_agree", ""),
        "species_guess": obs.get("species_guess", ""),
        "description": (obs.get("description") or "").replace("\n", " ").replace("\r", ""),
        "tag_list": "|".join(obs.get("tag_list") or []),
        "url": f"https://www.inaturalist.org/observations/{obs.get('id', '')}",
        "image_url": photos[0].get("url", "").replace("square", "medium") if photos else "",
        "sound_url": sounds[0].get("file_url", "") if sounds else "",
        "license_code": obs.get("license_code", ""),
        "captive_cultivated": obs.get("captive_cultivated", ""),
        "out_of_range": obs.get("out_of_range", ""),
        "native": conservation.get("native", ""),
        "introduced": conservation.get("introduced", ""),
        "endemic": conservation.get("endemic", ""),
        "threatened": taxon.get("threatened", ""),
    }


def save_csv(rows: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"CSV saved → {path} ({len(rows)} rows)")


def save_sqlite(rows: list[dict], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cols = ", ".join(f'"{c}" TEXT' for c in CSV_FIELDS)
    cur.execute(f'CREATE TABLE IF NOT EXISTS observations ({cols}, PRIMARY KEY ("id"))')
    placeholders = ", ".join("?" for _ in CSV_FIELDS)
    col_list = ", ".join(f'"{c}"' for c in CSV_FIELDS)
    for row in rows:
        values = [str(row.get(c, "")) for c in CSV_FIELDS]
        cur.execute(
            f'INSERT OR REPLACE INTO observations ({col_list}) VALUES ({placeholders})',
            values
        )
    conn.commit()
    count = cur.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    conn.close()
    log.info(f"SQLite saved → {path} ({count} total rows)")


def main():
    out_dir = Path(os.environ.get("DATA_DIR", "data"))
    csv_path = out_dir / "observations.csv"
    db_path = out_dir / "observations.db"

    all_taxa = [(name, "genus") for name in GENUS_TAXA] + \
               [(name, "species") for name in SPECIES_TAXA]

    all_rows: list[dict] = []
    seen_ids: set[str] = set()

    for taxon_name, rank in all_taxa:
        log.info(f"── Resolving {rank}: {taxon_name}")
        taxon_id = resolve_taxon_id(taxon_name)
        if taxon_id is None:
            log.warning(f"  Could not resolve '{taxon_name}' — skipping")
            time.sleep(REQUEST_DELAY)
            continue

        log.info(f"  taxon_id={taxon_id}")
        obs = fetch_observations_for_taxon(taxon_id, taxon_name)
        for row in obs:
            oid = str(row["id"])
            if oid not in seen_ids:
                seen_ids.add(oid)
                all_rows.append(row)
        time.sleep(REQUEST_DELAY)

    log.info(f"\nTotal unique observations: {len(all_rows)}")
    save_csv(all_rows, csv_path)
    save_sqlite(all_rows, db_path)

    # Write run summary
    summary_path = out_dir / "last_sync.txt"
    with open(summary_path, "w") as f:
        f.write(f"Last sync: {datetime.utcnow().isoformat()} UTC\n")
        f.write(f"Total observations: {len(all_rows)}\n")
        f.write(f"Taxa queried: {len(all_taxa)}\n")
    log.info("Done ✓")


if __name__ == "__main__":
    main()
