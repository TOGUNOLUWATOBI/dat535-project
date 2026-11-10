# Server storage maintenance

## Current policy: selected-provider ZIPs

The school VM has a 38 GiB root disk. After preparing full-month calendar
support, the server reported 23 GiB in Bronze and 3.5 GiB free. Successful
national ZIP removal was working; permanently extracted XML was the main cost.
The user approved moving existing validated snapshots to compressed ZIP storage.

Keep each snapshot's selected KOL + AKT XML and national stops inside a compact
ZIP. Keep manifests, source identity, lineage and validation reports beside it.
ZIP compression preserves the exact original XML bytes and NeTEx references.
Staging, Silver, Gold and the checkpoint registry remain local and unchanged by
the migration. National archives include other providers and are a separate
retention decision; original manual uploads are never deleted by this tool.

## Migrate existing XML without downloading again

Use the branch containing ZIP storage support, and let existing ingestion and
processing jobs finish before migration. Old checkouts do not know how to restore
ZIP-backed XML. Do not delete entire run folders or manually change their manifests.

Preview (no XML or manifests changed):

```bash
cd ~/dat535-project
python3 -B -u -m pipeline.storage.zip_bronze \
  --bronze-root /srv/dat535/data/bronze/entur
```

Apply the approved format migration:

```bash
python3 -B -u -m pipeline.storage.zip_bronze \
  --bronze-root /srv/dat535/data/bronze/entur --apply \
  --report /srv/dat535/data/metadata/zip-migration-1.json

df -h /srv/dat535/data
```

Use a new report filename on another invocation. The report records each source
manifest, date, logical XML bytes, retained ZIP bytes, skipped snapshots and
problems. Logical byte totals are not estimates of physical disk savings:
hardlinks may share storage. Use `df` to measure available space afterwards.

For each complete, validated snapshot, the tool:

1. Checks its passed validation report and every XML size and SHA-256 against
   the original lineage. Unexpected files, unsafe paths or changed XML block cleanup.
2. Builds a selected ZIP, or verifies a retained one. Checks the exact member
   set, sizes, CRCs and every decompressed XML SHA-256. It processes one snapshot
   at a time, rather than building all ZIPs before releasing any XML.
3. Saves a manifest backup under `storage_backups/`, then durably records
   `raw_storage="zip"`, `combined_zip`, its SHA-256 and compressed size.
4. Unlinks only that snapshot's known, verified XML files. Shared hardlinks in
   other snapshots remain intact. Checkpoints and derived outputs are untouched.

Failed/incomplete snapshots and their evidence remain. The failed February
source therefore still occupies storage; this tool does not silently discard it.
A problem with one manifest is reported and causes a nonzero exit status.
Already migrated snapshots are verified on a rerun. A ZIP published immediately
before an interruption can be adopted only after a full lineage match.

## Processing and recovery

`run_snapshot`, standalone XML conversion, calendar support checks, manual-import
duplicate verification and independent journey tracing understand ZIP storage.
They verify the ZIP and restore XML to its original paths under an exclusive
snapshot lock. All source paths, dates, IDs, references and content fingerprints
stay the same. Nested processing uses the same extraction.

The extraction remains until that snapshot's operation finishes. On success or
a Python exception, cleanup rechecks the archive and XML before releasing the
expanded copy. The ZIP remains authoritative even if Spark fails. A hard kill
can leave expanded XML; a later run verifies it and recovers. Changed cache files
or a damaged archive are preserved and reported, not silently removed.

Allow space for one full snapshot plus at least 1 GiB headroom before extraction;
Spark and new outputs may require additional space. The existing processing
checkpoints still verify completed Parquet outputs before reuse. Updated reader
code can require a new staging implementation and downstream attempts once;
the storage migration itself keeps the source fingerprint unchanged. Source hashes
are validated even when every stage can be reused. GUI and route-history reads
of existing Gold do not need raw XML extraction.

Low-level `load_sources`/`load_snapshot_sources` callers that read XML directly
must hold `pipeline.storage.zip_bronze.snapshot_xml(manifest, date)` around their
whole operation. Public processing entry points do this automatically.

## New ingestion

The scheduled workflow now supplies `--zip-only --remove-national-after-validation`.
Calendar-support acquisitions use the same retention policy. Successful national
downloads are removed only after selected data validation and ZIP verification;
failed sources remain. Both historical generation and polling expose `--zip-only`.
Manual upload also accepts `--zip-only` and continues to preserve the original upload.
The Python APIs retain their old expanded defaults for existing callers.

Merge the workflow change to the default branch to update scheduled jobs. Pulling
the branch in `~/dat535-project` updates manual commands, not the Actions checkout.

## Previous XML-only cleanup

`pipeline.bronze_storage` remains available for checksum-verified hardlink
consolidation of expanded datasets. Its `--discard-provider-zips` option only
applies to redundant ZIP copies: it explicitly refuses to discard an archive
marked `raw_storage="zip"`. Do not use an older version of that command after
migration; it lacks that protection. ZIP retention supersedes the earlier
XML-only policy for the school server.

The Group04 runner was stopped/uninstalled by the user and its binary/update
packages removed. The DAT535 runner under `/home/ubuntu/dat535-runner`, its service,
and active VS Code server installations remain required.
