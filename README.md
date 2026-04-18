# 🍄 Bryophilous Fungi Tracker

Automatically syncs all iNaturalist observations for **bryophilous fungi** (fungi that grow on bryophytes — mosses and liverworts) into a continuously-updated species sheet with full coordinate data and metadata.

## 📋 What's tracked

| Type | Taxa |
|------|------|
| Genera | Bryoscyphus, Bryochiton, Chromocyphella, Lamprospora, Lizonia, Octosporella, Epibryon, Bryocentria, Rimbachia, Eocronartium, Luteodiscus, Bryonectria, Loreleia, Helotium, Bryosphaeria, Potridiscus, Gloeopeziza, Coniochaeta, Bryobroma, Pithyella, Bryopistillaria |
| Species | 60+ individual species/varieties (see `scripts/sync_inaturalist.py` for full list) |

## 📁 Data files

| File | Description |
|------|-------------|
| `data/observations.csv` | All observations as a flat CSV — importable into Excel, QGIS, R, Python, etc. |
| `data/observations.db` | Same data as SQLite — query with DB Browser for SQLite or any SQL tool |
| `data/last_sync.txt` | Timestamp and count from the most recent sync |

## 📊 CSV columns

`id` · `uuid` · `quality_grade` · `observed_on` · `observed_time_zone` · `created_at` · `updated_at` · `taxon_id` · `taxon_name` · `taxon_rank` · `taxon_common_name` · `taxon_ancestry` · **`latitude`** · **`longitude`** · `positional_accuracy` · `place_guess` · `place_ids` · `obscured` · `geoprivacy` · `user_id` · `user_login` · `user_name` · `num_identification_agreements` · `num_identification_disagreements` · `identifications_most_agree` · `species_guess` · `description` · `tag_list` · `url` · `image_url` · `sound_url` · `license_code` · `captive_cultivated` · `out_of_range` · `threatened`

## ⚙️ How it works

A **GitHub Actions** workflow runs automatically every day at **06:00 UTC**:

1. Calls the iNaturalist API for each target taxon
2. Pages through all observations (no limit)
3. Deduplicates by observation ID
4. Writes `observations.csv` and `observations.db`
5. Commits the updated files back to this repo

No server required. No cost. Fully automatic.

## 🚀 Setup

### 1. Fork or clone this repo

```bash
git clone https://github.com/YOUR_USERNAME/bryophilous-fungi-tracker.git
cd bryophilous-fungi-tracker
```

### 2. Enable Actions

Go to your repo → **Actions** tab → click **"I understand my workflows, go ahead and enable them"**

### 3. Run the first sync

Actions tab → **"Sync Bryophilous Fungi from iNaturalist"** → **"Run workflow"**

The first run may take several minutes as it fetches everything. After that, it runs daily automatically.

### 4. Run locally (optional)

```bash
pip install -r requirements.txt
python scripts/sync_inaturalist.py
# Output written to data/
```

## 🔍 Querying the SQLite database

```bash
# Install DB Browser: https://sqlitebrowser.org/
# Or use the command line:
sqlite3 data/observations.db "SELECT taxon_name, COUNT(*) as n FROM observations GROUP BY taxon_name ORDER BY n DESC;"
sqlite3 data/observations.db "SELECT taxon_name, latitude, longitude, observed_on, url FROM observations WHERE latitude != '' ORDER BY observed_on DESC LIMIT 20;"
```

## 📝 Notes

- Observations with `obscured=true` will have randomised coordinates per iNaturalist policy
- The iNaturalist API is rate-limited; the script includes polite delays between requests
- `place_ids` are pipe-separated (`|`) iNaturalist place ID integers

## License

Data sourced from [iNaturalist](https://www.inaturalist.org) under their [terms of use](https://www.inaturalist.org/pages/terms). Observation data is licensed per each observation's `license_code` field.
