# NeTEx XML to staging Parquet

The first processing job reads XML directly with Spark 4's built-in XML reader
and writes Parquet before Silver cleaning and joins. Raw XML and ZIPs stay in
Bronze. There is no intermediate XML-to-JSON conversion.

The school server reports PySpark 4.2.0. It needs Java 17 or newer, with no
separate `spark-xml` connector. For a new development environment, install
`requirements-processing.txt` in a project virtual environment. The ingestion
commands still use only Python's standard library. Official references:
[XML reader](https://spark.apache.org/docs/4.2.0/sql-data-sources-xml.html),
[installation](https://spark.apache.org/docs/4.2.0/api/python/getting_started/install.html).

## Run a small test

After the new code has been pushed and pulled into `~/dat535-project`, run this
inside the existing `spark-env` on the school server:

```sh
python -B -u -m pipeline.processing.bronze.xml_to_parquet \
  --manifest /srv/dat535/data/bronze/entur/kol_akt_2026_01_09/manifest.json \
  --date 2026-01-15 \
  --sample-per-provider 1 \
  --output /srv/dat535/data/staging/entur/january-2026-sample-001
```

This selects the smallest file containing service journeys for each provider.
The result is marked `scope=sample`, not a complete January dataset. The default
Spark master is `local[2]`. `--master` can select another configured master, but
this first source resolver requires local filesystem paths visible to Spark.
It does not load HDFS/S3 manifests.

For all January provider line files, omit `--sample-per-provider` and use another
new destination:

```sh
python -B -u -m pipeline.processing.bronze.xml_to_parquet \
  --manifest /srv/dat535/data/bronze/entur/kol_akt_2026_01_09/manifest.json \
  --date 2026-01-15 \
  --output /srv/dat535/data/staging/entur/january-2026-provider-files-001
```

Locally, the historical manifest is
`entur_netex_snapshots/kol_agder_2026/metadata/manifest.json`. A complete
API/manual run's `metadata/manifest.json` is also accepted. That already identifies
its date; any supplied `--date` must match. Failed or duplicate ingestions are
not processed. Historical manifests require an explicit date.

Output must be a new directory outside the input source tree. Failed attempts
retain their partial outputs and a failed manifest. Downstream consumers must
check `metadata/manifest.json` for `status=complete` before using a run.

## Tables and lineage

Spark reads `CompositeFrame` with an explicit nested schema, then uses
`posexplode` to retain parent context and source order. Arrays remain arrays
even when a source has one child. This step does not deduplicate entities.

| Parquet directory | Row grain |
| --- | --- |
| `frames/` | One projected CompositeFrame from a provider line XML file |
| `lines/` | One Line or FlexibleLine occurrence within a ServiceFrame |
| `service_journeys/` | One ServiceJourney or FlexibleServiceJourney occurrence within a TimetableFrame |
| `passing_times/` | One ordered TimetabledPassingTime occurrence within a service journey |

These are Snappy-compressed Parquet datasets with Spark part files, not single
flat files. Entity values are nested under `record` in the three expanded tables.

Rows include snapshot date, provider, source file/member, XML hash, profile and
publication timestamp. Expanded entities carry CompositeFrame and parent frame
IDs/versions, plus parent and entity indices. Passing times also carry their
service journey ID/version and `passing_time_index`. Indices are zero-based
source array positions. An ID/version pair alone is not treated as a unique
occurrence.

IDs, versions, public codes, times and day offsets remain strings. This preserves
leading zeros and avoids prematurely interpreting overnight times. Snapshot date
is the observation date, not the operating/service date.

## Validation and current scope

Input requires a complete ingestion, passing reference validation and matching
per-file hashes. The first projection requires one CompositeFrame per provider
line file. Spark uses `FAILFAST`. Output row counts must match the selected
files' Bronze validation counts. Each Parquet dataset is read back to check its
saved count, and original XML hashes are checked again before completion.

Structured route, journey-pattern, stop, quay and calendar-definition tables
remain to implement. Shared XML and national stops stay in Bronze for those
projections. Other child content inside selected frames is kept in
`_unmapped_xml` where Spark's wildcard reader supports it. Those values are
reader-produced inner XML and may be normalized; they are not byte-for-byte
copies of the original containers. Undeclared attributes are outside the current
projection. Original XML remains authoritative.

This job does not expand calendars, resolve references through joins, normalize
overnight times, quarantine invalid rows or build Gold aggregates. Parquet is
the storage format here; writing it does not make the data Silver.

## Inspect and test

In a PySpark session:

```python
root = "/srv/dat535/data/staging/entur/january-2026-sample-001"
journeys = spark.read.parquet(root + "/service_journeys")
journeys.select("provider", "snapshot_date", "record._id", "record.LineRef._ref").show(truncate=False)
times = spark.read.parquet(root + "/passing_times")
times.select("service_journey_id", "passing_time_index",
             "record.ArrivalTime", "record.DepartureTime").show(truncate=False)
```

Run `python -B -m unittest discover -s tests -v` inside a Spark-enabled environment.
Without PySpark, the ingestion/source-selection tests run and the two actual
Spark integration tests are explicitly skipped. The Spark tests check reference
attributes, leading zeros, repeated IDs, parent context, stop order, overnight
offsets, paths containing punctuation/Unicode, and count-mismatch failures.

## Local verification — 28 October 2026

All 46 tests passed with PySpark 4.2.0, Python 3.11 and Java 17. A real sample
using one KOL and one AKT XML produced two frames, two lines, two service journeys
and 20 passing times, with matching Parquet read-back counts.

The January provider-file run then processed all 537 eligible XML files:

| Dataset | Verified rows |
| --- | ---: |
| Frames | 537 |
| Lines, including two FlexibleLine occurrences | 537 |
| Service journeys | 25,289 |
| Passing times | 768,701 |

All counts matched the existing Bronze reports. Source hashes were unchanged.
The conversion took about 28 seconds after input selection and Spark startup;
this is a local functional check, not a cluster benchmark. The 372 MB of selected
provider line XML produced approximately 32.4 MB of Parquet across the four
overlapping projections. This is not a lossless size comparison of the entire
NeTEx snapshot: shared definitions and unprojected attributes are outside scope.

The result is at
`data/staging/entur/january-2026-provider-files-verified/`, with a small evidence
record at `docs/audit/2026-10-28/xml_to_parquet_verification.json`. The first full
attempt failed source matching because Hadoop and Python encoded filename
punctuation differently. URI normalization was fixed and tested; that failed
attempt remains available rather than being overwritten. These tests ran on the
local Mac, not the school server. Pull the development branch
`ft/initial-setup-plus-initial-data-finding` to obtain this code. Execution on the
school server remains to verify.
