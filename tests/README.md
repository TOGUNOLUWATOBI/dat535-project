# Tests

Run from the repository root:

```bash
python -B -m unittest discover -s tests -t . -v
```

The suite targets the current `src/pipeline/Bronze`, `Staging`, `Silver` and
`Gold` packages. Tests of the removed `pipeline.ingestion`, `processing`,
`storage`, checkpoint and monthly-rebuild APIs have been retired. Their history
is retained in Git.

Without processing dependencies, the suite runs the standard-library tests and
explicitly skips the Spark and BigQuery/Parquet classes. For full coverage use
Python 3.11, Java 17 and `pip install -r requirements.txt`. Spark tests share one
local session and use temporary manifests and Parquet, with no server data.
BigQuery is mocked; no test downloads data or needs cloud credentials.

Coverage includes Bronze date/publication selection and file integrity, the
executor ZIP reader, Silver input provenance and calendar/DST decisions, Gold
inventory, event projection, punctuality boundaries, endpoint joins and gates,
plus workflow orchestration and daily history publication.

## Known Gold regressions

Five tests use `unittest.expectedFailure` to document previously reviewed defects:

| Behavior to fix | Test module |
| --- | --- |
| Keep authority filters when their columns are omitted from projection | `test_gold_inputs` |
| Count the same journey on separate operating dates separately in periods | `test_gold_metrics` |
| Align planned and reported journey keys when a dated ID is present | `test_gold_metrics` |
| Require a recorded time kind for a recorded observation | `test_gold_metrics` |
| Reconcile endpoint occurrences across passing-time and SHA-256 call IDs | `test_gold_journeys` |

These are correctness gaps, not passing assertions. Fixing a defect produces an
unexpected success until its expected-failure marker is removed. The performance
pass preserves existing outputs and does not change these metric semantics.

## Server GUI integration

The four `test_app` checks assert facts about the historical dataset and are
opt-in. They are separate from the workflow's date-independent GUI smoke checks:

```bash
DAT535_RUN_GUI_INTEGRATION=1 \
DAT535_GOLD_RUN=/srv/dat535/data/gold/kol/runs/gold-kol-53d-2026-v2 \
DAT535_SILVER_RUNS=/srv/dat535/data/silver/kol/runs \
~/dat535-gui-env/bin/python -B -m unittest tests.test_app -v
```
