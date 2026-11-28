# Gold performance measurements

Use a Linux VM with the project's processing and GUI environments. Measurements
write to a new directory and never replace an existing run. Inputs are existing
Silver tables; Bronze download time is outside this study.

Freeze two complete source copies before running: an unmodified baseline and a
candidate. Run them sequentially with the same dates and Spark settings:

```bash
~/spark-env/bin/python scripts/performance/measure_gold.py \
  --source-dir /srv/dat535/benchmarks/2026-11-28/code/baseline \
  --output-dir /srv/dat535/benchmarks/2026-11-28/baseline-01 \
  --revision 262a4e5 --run-id baseline-01 --dates 2026-11-19

~/spark-env/bin/python scripts/performance/measure_gold.py \
  --source-dir /srv/dat535/benchmarks/2026-11-28/code/candidate \
  --output-dir /srv/dat535/benchmarks/2026-11-28/candidate-01 \
  --revision CANDIDATE_REVISION --run-id candidate-01 --dates 2026-11-19

~/dat535-gui-env/bin/python scripts/performance/compare_gold.py \
  /srv/dat535/benchmarks/2026-11-28/baseline-01/gold-runs/baseline-01 \
  /srv/dat535/benchmarks/2026-11-28/candidate-01/gold-runs/candidate-01 \
  --output /srv/dat535/benchmarks/2026-11-28/comparison-01.json

~/spark-env/bin/python scripts/performance/summarize.py \
  /srv/dat535/benchmarks/2026-11-28
```

Use new output directories and run IDs for repetitions. Counterbalance execution
order rather than always running the candidate second. The summary only computes
paired medians after both measurements finish and their comparison passes.

Each measurement records the Gold source checksum, revision, input authorities,
command, versions, wall time, GNU time output, a sampled process-group RSS trace,
Spark event logs, gate status, table counts and console log. Spark event logs
contain the physical plans and job/task/I/O/spill metrics.

The comparator checks all table schemas and SHA-256 row multisets, including
duplicate multiplicity. It excludes only `gold_run_id` and normalizes ordering of
the Gold arrays/maps used for reasons and sampling metadata. All numeric values,
including approximate percentiles, must match exactly. A percentile mismatch
fails the check and needs investigation; it is never automatically accepted.

RSS is summed across processes in the benchmark process group. Shared pages may
be counted more than once, and one-second sampling may miss short peaks. It is
an approximate memory measure, not unique physical memory or JVM heap usage.
OS cache state is not cleared. Keep other VM workloads steady and report the
dataset, settings, run order, raw results and limitations with any speed claim.
