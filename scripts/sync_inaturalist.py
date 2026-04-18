#!/usr/bin/env python3
"""
Sync bryophilous fungi observations from iNaturalist API.
Fetches all observations for the target taxa and saves to CSV and SQLite.

Phenology additions (vs. original):
  - time_observed_at   : full ISO datetime when observer recorded it
  - month              : 1-12 integer
  - day_of_year        : 1-366
  - week_of_year       : ISO week 1-53
  - season             : Spring / Summer / Autumn / Winter
                         (hemisphere-aware — inferred from latitude)
  - hemisphere         : N / S / unknown

Also outputs:
  data/phenology_summary.csv  — observation counts per taxon x month
  data/phenology_season.csv   — observation counts per taxon x season
"""

import os
import csv
import time
import sqlite3
import logging
import requests
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Target taxa ────────────────────────────────────────────────────────────────
GENUS_TAXA = [
    # ── Original genera ────────────────────────────────────────────────────
    "Bryoscyphus", "Bryochiton", "Chromocyphella", "Lamprospora",
    "Lizonia", "Octosporella", "Epibryon", "Bryocentria", "Rimbachia",
    "Eocronartium", "Luteodiscus", "Bryonectria", "Loreleia", "Helotium",
    "Bryosphaeria", "Potridiscus", "Gloeopeziza", "Coniochaeta",
    "Bryobroma", "Pithyella", "Bryopistillaria",
    # ── Added from Döbbeler, Davison & Buck (2023) Herzogia 36: 305–370 ───
    "Acrospermum",       # on pleurocarpous mosses
    "Belonioscyphella",  # necrotrophic on mosses and liverworts
    "Bryorella",         # on hypnalean mosses
    "Bryotria",          # on Frullania
    "Chlorociboria",     # on Polytrichaceae
    "Dactylospora",      # on liverworts
    "Dawsomyces",        # interlamellar, Polytrichaceae
    "Didymella",         # on Riccia
    "Epicoccum",         # on Plagiochila
    "Filicupula",        # on Frullania (Pezizales)
    "Hilberina",         # on Sphagnum
    "Hypobryon",         # leaf-perforating, on Frullania
    "Laniatria",         # on Frullania kunzei
    "Leptomeliola",      # on Ptilidium
    "Mniaecia",          # green apothecia on liverworts
    "Muellerella",       # on Frullania and Radula
    "Neottiella",        # on Riccia and mosses (Pezizales)
    "Octospora",         # large genus on mosses (Pezizales)
    "Paruephaedria",     # on leafy liverworts
    "Periantria",        # perianth-perforating on Frullania
    "Potriphila",        # interlamellar, Polytrichaceae
    "Protothelenella",   # on Polytrichastrum sexangulare
    "Pseudomicrodochium",# conidial, on Nowellia/Ptilidium
    "Roseodiscus",       # necrotrophic on liverworts and mosses
    "Stemphylium",       # on Leptodictyum riparium
    "Trichosphaerella",  # on Ptilidium
    "Trizodia",          # cyanotrophic, on Sphagnum
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
    "Octospora orthotrichi",
    # ── Added: taxa missing from original list ─────────────────────────────
    "Bryobroma microcarpum",
    "Bryobroma velenovskyi",
    "Bryobroma microcarpum var. racomitrii",
    "Hilberina sphagnorum",
    "Epibryon diaphanum",
    "Epibryon hepaticicola",
    "Epibryon dicrani",
    "Epibryon interlamellare",
    "Octospora leucoloma",
    "Hyphodiscus delitescens",
    "Pithyella chalaudii",
    "Bryocentria brongniartii",
    "Hymenoscyphus vasaensis",
    "Rickenella swartzii",
    # Family-level — iNat resolves these fine via taxa search
    "Coniochaetaceae",
    # ── Added from Döbbeler, Davison & Buck (2023) Herzogia 36: 305–370 ───
    # Acrospermum
    "Acrospermum adeanum",
    # Belonioscyphella
    "Belonioscyphella hypnorum",
    # Bryocentria (additional species)
    "Bryocentria biannulata",
    "Bryocentria chrysothrix",
    "Bryocentria lusor",
    "Bryocentria navicula",
    "Bryocentria pachydesma",
    "Bryocentria pentamera",
    # Bryochiton (additional species)
    "Bryochiton heliotropicus",
    "Bryochiton perpusillus",
    # Bryonectria (species)
    "Bryonectria anisopoda",
    "Bryonectria aphanes",
    "Bryonectria callicarpa",
    "Bryonectria cuneifera",
    "Bryonectria hylocomii",
    "Bryonectria phyllogena",
    # Bryorella
    "Bryorella acrogena",
    # Bryoscyphus (additional species)
    "Bryoscyphus hyalotectus",
    # Bryotria
    "Bryotria adelpha",
    "Bryotria lagodes",
    "Bryotria urophora",
    # Chlorociboria
    "Chlorociboria lamellicola",
    # Dawsomyces
    "Dawsomyces subinvisibilis",
    # Didymella
    "Didymella hepaticarum",
    # Epicoccum
    "Epicoccum plagiochilae",
    # Epibryon (additional species)
    "Epibryon arachnoideum",
    "Epibryon harrisii",           # new species described in this paper
    "Epibryon hypophyllum",
    "Epibryon intercapillare",
    "Epibryon intercellulare",
    "Epibryon pogonati-urnigeri",
    "Epibryon ventrale",
    # Filicupula
    "Filicupula cyanopoda",
    "Filicupula sororia",
    # Gloeopeziza (additional species)
    "Gloeopeziza interlamellaris",
    # Hilberina
    "Hilberina sphagni",
    # Hypobryon
    "Hypobryon bacillare",
    "Hypobryon florentinum",
    # Laniatria
    "Laniatria myxostoma",
    # Leptomeliola
    "Leptomeliola ptilidii",
    # Lizonia (additional species)
    "Lizonia emperigonia",
    "Lizonia sexangularis",
    # Muellerella
    "Muellerella frullaniae",
    "Muellerella rubescens",
    # Octospora (additional species)
    "Octospora ithacaensis",
    # Octosporella (additional species)
    "Octosporella brevibarbata",
    "Octosporella caudifera",
    "Octosporella imitatrix",
    "Octosporella jungermanniarum",
    "Octosporella ornithocephala",
    # Paruephaedria
    "Paruephaedria heimerlii",
    # Periantria
    "Periantria bellacaptiva",
    "Periantria frullaniae",
    # Pithyella (additional species)
    "Pithyella trigona",
    # Potriphila
    "Potriphila navicularis",
    # Protothelenella
    "Protothelenella polytrichi",
    # Pseudomicrodochium
    "Pseudomicrodochium bryophilum",
    # Roseodiscus
    "Roseodiscus subcarneus",
    # Stemphylium
    "Stemphylium botryosum",
    # Trichosphaerella
    "Trichosphaerella goniospora",
    # Trizodia
    "Trizodia acrobia",
    # Bryocentria pachydesma — new sp. described in paper (Alabama)
    # (already listed above)
]

# ── iNaturalist API ────────────────────────────────────────────────────────────
INAT_BASE = "https://api.inaturalist.org/v1"
PAGE_SIZE = 200
REQUEST_DELAY = 1.0  # seconds between requests (polite rate limiting)

# ── Column schema ──────────────────────────────────────────────────────────────
CSV_FIELDS = [
    # Core identifiers
    "id", "uuid", "quality_grade",
    # Time — original fields
    "observed_on", "observed_time_zone", "created_at", "updated_at",
    # Time — new phenology fields
    "time_observed_at",
    "month", "day_of_year", "week_of_year", "season", "hemisphere",
    # Taxon
    "taxon_id", "taxon_name", "taxon_rank", "taxon_common_name",
    "taxon_ancestry",
    # Location
    "latitude", "longitude", "positional_accuracy",
    "place_guess", "place_ids",
    "obscured", "geoprivacy",
    # Observer
    "user_id", "user_login", "user_name",
    # Identification
    "num_identification_agreements", "num_identification_disagreements",
    "identifications_most_agree", "species_guess",
    # Content
    "description", "tag_list", "url", "image_url", "sound_url",
    # Flags
    "license_code", "captive_cultivated", "out_of_range",
    "native", "introduced", "endemic", "threatened",
]

MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
SEASONS = ["Spring", "Summer", "Autumn", "Winter"]


# ── Phenology helpers ──────────────────────────────────────────────────────────

def infer_hemisphere(lat_str: str) -> str:
    """Return 'N', 'S', or 'unknown' from a latitude string."""
    try:
        return "N" if float(lat_str) >= 0 else "S"
    except (ValueError, TypeError):
        return "unknown"


def infer_season(month: int, hemisphere: str) -> str:
    """
    Meteorological seasons (3-month blocks).
    Northern:  Mar-May = Spring, Jun-Aug = Summer,
               Sep-Nov = Autumn,  Dec-Feb = Winter
    Southern hemisphere seasons are flipped.
    """
    if not month:
        return ""
    n_season = {
        12: "Winter", 1: "Winter",  2: "Winter",
        3:  "Spring", 4: "Spring",  5: "Spring",
        6:  "Summer", 7: "Summer",  8: "Summer",
        9:  "Autumn", 10: "Autumn", 11: "Autumn",
    }[month]
    if hemisphere == "S":
        return {"Winter": "Summer", "Summer": "Winter",
                "Spring": "Autumn",  "Autumn": "Spring"}[n_season]
    return n_season


def derive_phenology(observed_on: str, lat_str: str) -> dict:
    """Return all phenology columns from observed_on (YYYY-MM-DD) + latitude."""
    out = {"month": "", "day_of_year": "", "week_of_year": "",
           "season": "", "hemisphere": ""}
    if not observed_on:
        return out
    try:
        d = date.fromisoformat(observed_on)
        hemi = infer_hemisphere(lat_str)
        out["month"]        = d.month
        out["day_of_year"]  = d.timetuple().tm_yday
        out["week_of_year"] = d.isocalendar()[1]
        out["season"]       = infer_season(d.month, hemi)
        out["hemisphere"]   = hemi
    except ValueError:
        pass
    return out


# ── iNat API calls ─────────────────────────────────────────────────────────────

def resolve_taxon_id(name: str) -> int | None:
    """Return iNaturalist taxon_id for a name, or None on failure."""
    try:
        r = requests.get(
            f"{INAT_BASE}/taxa",
            params={"q": name, "per_page": 5, "is_active": "true"},
            timeout=20,
        )
        r.raise_for_status()
        results = r.json().get("results", [])
        for t in results:
            if t["name"].lower() == name.lower():
                return t["id"]
        return results[0]["id"] if results else None
    except Exception as e:
        log.warning(f"Taxon lookup failed for '{name}': {e}")
        return None


def fetch_observations_for_taxon(taxon_id: int, taxon_name: str) -> list[dict]:
    """Page through all observations for a taxon_id and return parsed rows."""
    all_obs: list[dict] = []
    page = 1
    while True:
        try:
            r = requests.get(
                f"{INAT_BASE}/observations",
                params={
                    "taxon_id": taxon_id,
                    "per_page": PAGE_SIZE,
                    "page": page,
                    "order": "desc",
                    "order_by": "created_at",
                    "include_new_projects": "true",
                },
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.error(f"Request error for {taxon_name} page {page}: {e}")
            break

        results = data.get("results", [])
        total   = data.get("total_results", 0)
        log.info(f"  {taxon_name}: page {page} — {len(results)} obs (total={total})")

        all_obs.extend(parse_observation(obs) for obs in results)

        if len(all_obs) >= total or not results:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    return all_obs


def parse_observation(obs: dict) -> dict:
    """Flatten an iNaturalist observation JSON into a CSV row dict."""
    taxon  = obs.get("taxon") or {}
    user   = obs.get("user")  or {}
    loc    = obs.get("location") or ""

    lat, lon = "", ""
    if loc and "," in loc:
        lat, lon = [s.strip() for s in loc.split(",", 1)]

    photos    = obs.get("photos") or []
    sounds    = obs.get("sounds") or []
    place_ids = obs.get("place_ids") or []
    conserv   = taxon.get("conservation_status") or {}

    common = (taxon.get("preferred_common_name")
              or taxon.get("english_common_name")
              or "")

    observed_on      = obs.get("observed_on", "")
    time_observed_at = obs.get("time_observed_at", "")  # full datetime or ""
    pheno            = derive_phenology(observed_on, lat)

    return {
        "id":            obs.get("id", ""),
        "uuid":          obs.get("uuid", ""),
        "quality_grade": obs.get("quality_grade", ""),
        # Time
        "observed_on":        observed_on,
        "observed_time_zone": obs.get("observed_time_zone", ""),
        "created_at":         obs.get("created_at", ""),
        "updated_at":         obs.get("updated_at", ""),
        "time_observed_at":   time_observed_at,
        # Phenology
        "month":        pheno["month"],
        "day_of_year":  pheno["day_of_year"],
        "week_of_year": pheno["week_of_year"],
        "season":       pheno["season"],
        "hemisphere":   pheno["hemisphere"],
        # Taxon
        "taxon_id":          taxon.get("id", ""),
        "taxon_name":        taxon.get("name", ""),
        "taxon_rank":        taxon.get("rank", ""),
        "taxon_common_name": common,
        "taxon_ancestry":    taxon.get("ancestry", ""),
        # Location
        "latitude":            lat,
        "longitude":           lon,
        "positional_accuracy": obs.get("positional_accuracy", ""),
        "place_guess":         obs.get("place_guess", ""),
        "place_ids":           "|".join(str(p) for p in place_ids),
        "obscured":            obs.get("obscured", ""),
        "geoprivacy":          obs.get("geoprivacy", ""),
        # Observer
        "user_id":    user.get("id", ""),
        "user_login": user.get("login", ""),
        "user_name":  user.get("name", ""),
        # Identification
        "num_identification_agreements":    obs.get("num_identification_agreements", ""),
        "num_identification_disagreements": obs.get("num_identification_disagreements", ""),
        "identifications_most_agree":       obs.get("identifications_most_agree", ""),
        "species_guess":                    obs.get("species_guess", ""),
        # Content
        "description": (obs.get("description") or "").replace("\n", " ").replace("\r", ""),
        "tag_list":    "|".join(obs.get("tag_list") or []),
        "url":         f"https://www.inaturalist.org/observations/{obs.get('id', '')}",
        "image_url":   photos[0].get("url", "").replace("square", "medium") if photos else "",
        "sound_url":   sounds[0].get("file_url", "") if sounds else "",
        # Flags
        "license_code":       obs.get("license_code", ""),
        "captive_cultivated": obs.get("captive_cultivated", ""),
        "out_of_range":       obs.get("out_of_range", ""),
        "native":             conserv.get("native", ""),
        "introduced":         conserv.get("introduced", ""),
        "endemic":            conserv.get("endemic", ""),
        "threatened":         taxon.get("threatened", ""),
    }


# ── Output writers ─────────────────────────────────────────────────────────────

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
    cur  = conn.cursor()
    cols = ", ".join(f'"{c}" TEXT' for c in CSV_FIELDS)
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS observations ({cols}, PRIMARY KEY ("id"))'
    )
    ph  = ", ".join("?" for _ in CSV_FIELDS)
    col = ", ".join(f'"{c}"' for c in CSV_FIELDS)
    for row in rows:
        cur.execute(
            f'INSERT OR REPLACE INTO observations ({col}) VALUES ({ph})',
            [str(row.get(c, "")) for c in CSV_FIELDS],
        )
    conn.commit()
    count = cur.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    conn.close()
    log.info(f"SQLite saved → {path} ({count} total rows)")


def save_phenology_summary(rows: list[dict], out_dir: Path):
    """
    Write two phenology pivot tables:
      phenology_summary.csv  — taxon x month  (columns: Jan … Dec + total)
      phenology_season.csv   — taxon x season (Spring / Summer / Autumn / Winter)

    Month counts use calendar month (not flipped by hemisphere) so you can
    split by hemisphere column in your own analysis if needed.
    Season counts ARE hemisphere-corrected (Autumn in the S = March–May, etc.)
    """
    month_counts: dict[str, dict[int, int]]   = defaultdict(lambda: defaultdict(int))
    season_counts: dict[str, dict[str, int]]  = defaultdict(lambda: defaultdict(int))

    for row in rows:
        taxon = row.get("taxon_name") or "Unknown"
        m = row.get("month")
        s = row.get("season")
        if m:
            try:
                month_counts[taxon][int(m)] += 1
            except (ValueError, TypeError):
                pass
        if s:
            season_counts[taxon][str(s)] += 1

    # Month pivot
    month_path   = out_dir / "phenology_summary.csv"
    month_fields = ["taxon_name", "total"] + MONTHS
    with open(month_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=month_fields)
        w.writeheader()
        for taxon in sorted(month_counts):
            mc      = month_counts[taxon]
            row_out = {"taxon_name": taxon, "total": sum(mc.values())}
            for i, mname in enumerate(MONTHS, start=1):
                row_out[mname] = mc.get(i, 0)
            w.writerow(row_out)
    log.info(f"Phenology (month) saved → {month_path}")

    # Season pivot
    season_path   = out_dir / "phenology_season.csv"
    season_fields = ["taxon_name", "total"] + SEASONS
    with open(season_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=season_fields)
        w.writeheader()
        for taxon in sorted(season_counts):
            sc      = season_counts[taxon]
            row_out = {"taxon_name": taxon, "total": sum(sc.values())}
            for s in SEASONS:
                row_out[s] = sc.get(s, 0)
            w.writerow(row_out)
    log.info(f"Phenology (season) saved → {season_path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    out_dir  = Path(os.environ.get("DATA_DIR", "data"))
    csv_path = out_dir / "observations.csv"
    db_path  = out_dir / "observations.db"

    all_taxa = (
        [(name, "genus")   for name in GENUS_TAXA] +
        [(name, "species") for name in SPECIES_TAXA]
    )

    all_rows: list[dict] = []
    seen_ids: set[str]   = set()

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
    save_phenology_summary(all_rows, out_dir)

    summary_path = out_dir / "last_sync.txt"
    with open(summary_path, "w") as f:
        f.write(f"Last sync: {datetime.utcnow().isoformat()} UTC\n")
        f.write(f"Total observations: {len(all_rows)}\n")
        f.write(f"Taxa queried: {len(all_taxa)}\n")
    log.info("Done ✓")


if __name__ == "__main__":
    main()
