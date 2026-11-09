# Manual ZIP ingestion

`pipeline.ingestion.ingest_local_zip` imports an existing national Entur ZIP
or a previously selected KOL + AKT ZIP. It uses no network requests and needs
only Python 3.11 or newer. The same `ingest_zip` function can be called by the
future upload GUI.

From the cloned project directory:

```sh
python3 -B -u -m pipeline.ingestion.ingest_local_zip \
  --source entur_netex_snapshots/kol_agder_2026/zip/KOL_AKT_netex_20260115.zip \
  --date 2026-01-15
```

The date is required. It is a user-supplied claim, not inferred from the filename
or verified against Entur's archive metadata. Future ordinary-line publication
dates are checked, but that alone cannot prove an upload's historical date.

For an existing archive on the school server, after deploying this module:

```sh
python3 -B -u -m pipeline.ingestion.ingest_local_zip \
  --source /srv/dat535/data/bronze/entur/kol_akt_2026_01_09/zip/KOL_AKT_netex_20260115.zip \
  --date 2026-01-15 \
  --output /srv/dat535/data/bronze/entur/manual-january-check-001
```

An explicit output directory must be new. Without `--output`, a unique run is
created below `data/bronze/entur/`. Server invocations should supply persistent
storage outside the checkout.

## What is kept and checked

The input is never modified. A new import copies its original ZIP byte for byte,
records SHA-256, MD5 and byte size, checks every member's CRC, and rejects unsafe
paths, symbolic links and duplicate member names. It then reuses the existing
provider extraction and reference validator. Selected XML is not rewritten.

The archive must contain line XML and ordinary shared data for every requested
provider, plus `_stops.xml`. Flexible shared data is retained when present.
Missing references, malformed XML, bad CRCs or missing required files fail the
import. Repeated source ID/version occurrences remain source warnings. These
are XML/reference checks, not full NeTEx XSD, profile or calendar validation.

```text
bronze/entur/
├── _manual_ingestions.sqlite3       Metadata index for successful manual imports
└── manual-RUN/
    ├── uploads/original.zip        Exact uploaded bytes
    ├── xml/DATE/                   Original selected XML and national stops
    ├── zip/                        Compact selected snapshot
    └── metadata/
        ├── manifest.json           Source checksum, date, providers and status
        ├── lineage.json            Member paths, sizes, CRCs and SHA-256 hashes
        └── validation.json         XML and reference checks
```

Output paths in a run's metadata are relative to its directory. Failed imports
keep their copied ZIP, partial outputs and a failed manifest; they are not
registered as successful. Downstream processing must accept only `complete` or
`complete_with_warnings` imports. Retrying a failure requires a new run directory.

## Repeat uploads

The duplicate key is **original ZIP SHA-256 + claimed snapshot date + provider
selection**. Renaming the same file or reordering provider arguments does not
create another successful import. Different dates and provider selections are
separate observations; a new date still goes through validation.

The catalog checks the previous raw archive, selected ZIP and materialized XML
before treating it as a duplicate. A duplicate creates a new metadata-only run
with status `duplicate`; `duplicate_of` points to the original manifest relative
to the new run. No second ZIP copy or XML extraction is created. A damaged or
missing original fails clearly rather than being silently reused.

The default catalog is `_manual_ingestions.sqlite3` in the output directory's
parent. Use `--catalog /path/to/shared.sqlite3` when imports use different parent
directories but should share duplicate checks. The catalog holds a write lock
through import and registration, so concurrent imports cannot register the same
key twice. Another importer waits up to 120 seconds, then fails if it cannot
acquire the lock. SQLite releases the lock and rolls back uncommitted catalog
changes when a process exits; existing run files remain available for inspection.

This is a small ingestion metadata index, not the analytical database. Keep the
catalog and its imports in place: the index records absolute manifest paths.
Relocating them needs a catalog migration or a new explicitly chosen catalog.

Duplicate detection currently covers this manual path only. It does not scan
the earlier historical directories or stop repeated API downloads. Repacked ZIPs
with identical XML but different ZIP bytes are also separate inputs. Entity-level
deduplication and shared API/manual ingestion identity remain later work.

## Verification — 28 October 2026

All 40 ingestion tests pass on Python 3.11 and 3.14. Tests cover national and
selected ZIPs, retained references, unsafe paths, bad CRCs, missing dependencies,
failed retries, modified previous outputs and concurrent duplicate uploads.

We also imported the real January ZIP locally and repeated the upload. See the
ignored audit record at `audit/2026-10-28/manual_zip_ingestion_verification.json`
for the counts, original checksum and duplicate result. This is a test of existing
January data, not additional historical coverage. The module has not yet been
deployed to the school server or connected to a GUI.
