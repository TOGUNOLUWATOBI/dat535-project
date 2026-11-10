# Server storage maintenance

The server report on 10 November showed a 38 GiB root disk with 4.9 GiB free.
Bronze used 20 GiB; staging, Silver and Gold together used about 1.3 GiB.
The 23 remaining ZIP files totalled 1.37 GiB. Repeated 8/9 November runs had
roughly 0.8 GiB of extracted XML each. ZIP removal alone cannot solve that growth.

## Verify and consolidate duplicate inputs

Run from the checked-out project. These commands require no Spark session:

```bash
python3 -B -u -m pipeline.bronze_storage \
  --bronze-root /srv/dat535/data/bronze/entur \
  --discard-provider-zips --report /tmp/dat535-storage-preview.json
```

The default is a dry run. It reads completed ingestion manifests and lineage,
groups expected identical files, then verifies their SHA-256 hashes. Running
and failed snapshots are excluded. Only identical files on the same filesystem
with matching ownership and permissions are consolidated. Every raw XML path
and its bytes remain; duplicate paths share storage through hardlinks. Existing
source references and checkpoint hashes remain valid. Inputs must stay immutable.

After reviewing the estimate, apply the same verified cleanup:

```bash
python3 -B -u -m pipeline.bronze_storage \
  --bronze-root /srv/dat535/data/bronze/entur \
  --discard-provider-zips --apply \
  --report /srv/dat535/data/metadata/storage-cleanup.json

df -h /srv/dat535/data
```

Provider ZIP copies are discarded only after their recorded ZIP checksum,
member CRCs and every extracted XML checksum match. The manifest records
XML-only retention before removing the copy. Historical resume verifies XML
in that mode; it does not require an intentionally discarded compact ZIP.
Manual uploads and their archives are excluded from ZIP removal. Damaged,
failed and incomplete inputs are left intact and reported.

Do not delete entire ingestion runs: the registry and lineage may reference
their paths. Do not remove `pipeline_state.sqlite3`, current Parquet outputs,
or active runner/VS Code installations to make room.

## Prevent repeat copies

Scheduled ingestion now uses `--xml-only --remove-national-after-validation`:
the national ZIP is temporary, no compact provider ZIP is created, and the
original selected XML remains. Failed national downloads are retained as
evidence. The workflow consolidates verified duplicate Bronze files after
successful ingestion and saves a report with the run metadata.

New monthly support datasets also use XML-only mode. A historical dataset can
explicitly opt into it with `generate_provider_dataset --xml-only`. Existing
retention choices are respected on resume; use the verified cleanup above for
older compact copies.

The two runner services on this server were both active. The user selected
the Group04 runner for decommissioning; its installation is
`/home/ubuntu/actions-runner/actions-runner`. Stop/uninstall only its exact
service and preserve `_work` until any unique project data has been checked.
The DAT535 runner under `/home/ubuntu/dat535-runner` must remain. Both observed
VS Code server versions were in use, so their directories are not blanket
cleanup targets.
