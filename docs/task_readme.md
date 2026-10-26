# DAT535 — Kolumbus and Agder Project Task Breakdown

6-week plan · 2 developers + AI

This is the task breakdown I recommend for you and your partner, organized into epics, user stories, technical tasks, ownership, and deliverables.

The workload is divided so that both of you can work independently without constantly blocking each other, while still collaborating on the most important part of the course: Silver processing using PySpark.

## 1. Project scope

Project title: Kolumbus and Agder Historical Service Evolution and Analytics Platform

Problem we're solving: Kolumbus and Agder bus routes, numbers, stopping patterns, and timetables change over time. We want to build a data platform that preserves these changes and makes it possible to compare planned public-transport services historically.

| Item                 | Decision                                          |
| -------------------- | ------------------------------------------------- |
| Region               | Rogaland and Agder: Kolumbus (`KOL`) + Agder Kollektivtrafikk (`AKT`) |
| Historical period    | 15 January – 15 September 2026                   |
| Historical ingestion | Nine monthly NeTEx snapshots, on the 15th of each month |
| Other ingestion      | Manual ZIP upload, API polling, daily/weekly cron |
| Processing           | PySpark + Spark SQL                               |
| Architecture         | Bronze → Silver → Gold                            |
| Interface            | Streamlit GUI                                     |
| Deployment           | Group PySpark server                              |
| Development duration | 6 weeks                                           |

## 2. Responsibility split

## Developer A

Historical data & analytics lead

Owns historical data acquisition, NeTEx parsing, core Silver transformations, route history and Gold analytics.

Main focus: dataset correctness, modeling, Spark processing.

## Developer B

Ingestion, automation & GUI lead

Owns manual uploads, API polling, cron jobs, pipeline orchestration, deployment and GUI.

Main focus: operational reliability, application and integration.

Both developers: Silver quality validation, architecture decisions, integration testing, PySpark performance analysis and the final report.

# 3. Epic and task backlog

The checkboxes below are your implementation checklist. Each user story explains the purpose of the work.

## Phase 1 — Source data and ingestion

### EPIC 01 — Project setup and planning

User story: As a data engineering team, we want a clearly defined project scope and development environment so that we can build and deploy a reproducible pipeline.

Week 1 · Both developers

| Task ID | Task                                                  | Owner |
| ------- | ----------------------------------------------------- | ----- |
| T01     | Create GitHub repository and issue board              | B     |
| T02     | Set up Python, PySpark and project dependencies       | B     |
| T03     | Finalize data sources and nine-month coverage          | A     |
| T04     | Define Bronze, Silver and Gold table specifications   | A     |
| T05     | Prepare architecture diagram and project definition   | Both  |
| T06     | Define directory structure and dev/prod configuration | B     |

Done when: Both developers can run a basic PySpark job, use the repository, and agree on the table schemas and scope.

### EPIC 02 — Historical dataset acquisition

User story: As a transport analyst, I want access to nine months of historical Kolumbus and Agder timetables so that I can investigate how services changed over time.

Weeks 1–2 · Developer A

| Task ID | Task                                                                 | Owner |
| ------- | -------------------------------------------------------------------- | ----- |
| T07     | Connect to the Entur historical NeTEx archive listing                | A     |
| T08     | Select nine monthly snapshots from January through September 2026 | A     |
| T09     | Build historical ZIP downloader                                      | A     |
| T10     | Filter/extract KOL and AKT files and required shared reference data     | A     |
| T11     | Create snapshot manifest: source date, file size, checksum, status   | A     |
| T12     | Validate downloaded archives and missing dates                       | A     |

Done when: Selected historical archives are reproducibly available and can be re-downloaded or reprocessed.

### EPIC 03 — Manual upload and API ingestion

User story: As a platform operator, I want to upload historical files and automatically collect API updates so that data can enter the system through multiple ingestion methods.

Weeks 2–3 · Developer B

| Task ID | Task                                                               | Owner |
| ------- | ------------------------------------------------------------------ | ----- |
| T13     | Implement manual ZIP ingestion via Python CLI/service              | B     |
| T14     | Validate ZIP integrity, format and duplicate uploads               | B     |
| T15     | Store original uploaded archive with ingestion metadata            | B     |
| T16     | Build Entur API polling client for KOL and AKT                                 | B     |
| T17     | Implement request interval, timeout, retry and rate-limit handling | B     |
| T18     | Store raw API responses with source and ingestion timestamps       | B     |
| T19     | Implement ingestion success/failure logs                           | B     |

Done when: Both manual ingestion and repeated API polling can write raw data to Bronze without creating duplicate logical ingestions.

### EPIC 04 — Bronze and NeTEx parsing

User story: As a data engineer, I want to preserve source records and convert complex XML into structured staging data so that they can be processed reliably using PySpark.

Weeks 2–3 · Developer A, with B integrating the ingestion paths

| Task ID | Task                                                        | Owner |
| ------- | ----------------------------------------------------------- | ----- |
| T20     | Implement Bronze storage layout for ZIP, XML and API data   | A     |
| T21     | Extract lines, routes and service journeys from NeTEx       | A     |
| T22     | Extract journey patterns, stop references and passing times | A     |
| T23     | Extract calendars, validity periods and shared definitions  | A     |
| T24     | Convert parsed entities into structured staging tables      | A     |
| T25     | Preserve source IDs and file/snapshot lineage               | A     |
| T26     | Test record counts and parser failures                      | Both  |

Done when: At least one complete Kolumbus and Agder snapshot is transformed into structured records, with the original archive preserved and all output rows traceable to their source.

## Phase 2 — Silver and historical analytics

This is the most academically important phase. Both developers must contribute PySpark code here, even though A leads the data model.

### EPIC 05 — Silver preprocessing and data quality

User story: As a transport analyst, I want accurate, standardized and connected timetable data so that my comparisons are not affected by inconsistent timestamps, duplicates, or unresolved references.

Weeks 3–4 · Both developers

| Task ID | Task                                                           | Owner |
| ------- | -------------------------------------------------------------- | ----- |
| T27     | Define Silver table grains, keys and schemas                   | A     |
| T28     | Profile missing values, duplicated keys and data types         | B     |
| T29     | Standardize columns, IDs, datatypes and timestamps             | A     |
| T30     | Handle overnight trips and departure-day offsets               | A     |
| T31     | Handle flexible services and special timetable fields          | B     |
| T32     | Implement appropriate deduplication rules                      | B     |
| T33     | Join journeys, routes, stops and service calendars             | A     |
| T34     | Validate referential integrity and prevent join multiplication | A     |
| T35     | Quarantine invalid records with reason codes                   | B     |
| T36     | Produce data-quality metrics before and after Silver           | B     |
| T37     | Write unit tests for transformations and Spark SQL checks      | Both  |

Done when: The cleaned Silver data is reusable and documented, with passing schema tests, validated joins, and quantitative quality results.

### EPIC 06 — Historical route change detection

User story: As a transport analyst, I want to identify bus-number, route, and timetable changes so that I can accurately compare Kolumbus and Agder services before and after major network reorganizations.

Week 4 · Developer A, with B testing

| Task ID | Task                                                           | Owner |
| ------- | -------------------------------------------------------------- | ----- |
| T38     | Compare line and route definitions across historical snapshots | A     |
| T39     | Detect added, changed and retired routes                       | A     |
| T40     | Compare stop sequences and journey patterns                    | A     |
| T41     | Implement SCD Type 2 / versioned route dimensions              | A     |
| T42     | Create mappings for verified route renumberings                | A     |
| T43     | Test effective dates and historical timetable joins            | B     |

Done when: We can demonstrate a documented route change in Kolumbus or Agder while retaining the earlier version and producing correct results for historical service dates.

### EPIC 07 — Gold analytical tables

User story: As a transport analyst, I want aggregated information about planned bus services so that I can understand changes in service frequency and network coverage.

Weeks 4–5 · Developer A

| Task ID | Task                                                         | Owner |
| ------- | ------------------------------------------------------------ | ----- |
| T44     | Calculate scheduled departures per route and date            | A     |
| T45     | Create weekly/monthly planned service-frequency metrics      | A     |
| T46     | Create route addition, retirement and modification summaries | A     |
| T47     | Calculate stop coverage and changes over time                | A     |
| T48     | Validate Gold aggregates against Silver source counts        | Both  |

Done when: Gold datasets support comparisons across selected dates and are reconciled against the underlying timetable records.

## Phase 3 — Automation, GUI and deployment

### EPIC 08 — Scheduled pipeline orchestration

User story: As a platform operator, I want data ingestion and processing to happen automatically so that the data platform remains current without manual execution.

Weeks 3–5 · Developer B

| Task ID | Task                                                    | Owner |
| ------- | ------------------------------------------------------- | ----- |
| T49     | Configure weekly GitHub Actions ingestion on the school Linux runner | B     |
| T50     | Configure periodic API polling                          | B     |
| T51     | Configure weekly snapshot comparison and reconciliation | B     |
| T52     | Orchestrate Bronze → Silver → Gold dependencies         | B     |
| T53     | Add job locking, safe reruns and failure recovery       | B     |

Done when: Scheduled jobs run without manual intervention, and failed upstream processing prevents invalid downstream publication.

### EPIC 09 — Streamlit GUI

User story: As a researcher or transport analyst, I want an interactive application so that I can explore historical Kolumbus and Agder services without needing to understand the underlying XML or PySpark code.

Week 5 · Developer B

| Task ID | Task                                      | Owner |
| ------- | ----------------------------------------- | ----- |
| T54     | Create Streamlit app with navigation      | B     |
| T55     | Implement manual ZIP upload interface     | B     |
| T56     | Create historical date/route filters      | B     |
| T57     | Build before-and-after route comparison   | B     |
| T58     | Build service-frequency visualizations    | B     |
| T59     | Display data-quality and ingestion status | B     |
| T60     | Connect GUI to published Gold datasets    | B     |

Done when: A user can upload a ZIP and explore real Gold data, including changes in scheduled services around a known route reorganization.

### EPIC 10 — Deployment and CI/CD

User story: As a data engineer, I want an automated and reproducible deployment so that another engineer can run and maintain the platform.

Weeks 1–6, implemented incrementally · Developer B

| Task ID | Task                                                  | Owner |
| ------- | ----------------------------------------------------- | ----- |
| T61     | Package Python/PySpark dependencies and configuration | B     |
| T62     | Configure GitHub Actions for linting and tests        | B     |
| T63     | Prepare deployment scripts for the Spark server       | B     |
| T64     | Set up environment-specific paths and credentials     | B     |
| T65     | Configure application/job logging and monitoring      | B     |
| T66     | Test recovery and redeployment                        | B     |

Done when: The platform can be configured and deployed consistently, with passing automated checks and visible operational status.

### EPIC 11 — Testing, performance and final report

User story: As a course assessor or another engineer, I want evidence that the platform is correct, scalable and reproducible so that its engineering quality can be evaluated.

Weeks 5–6 · Both developers

| Task ID | Task                                                        | Owner |
| ------- | ----------------------------------------------------------- | ----- |
| T67     | Run end-to-end ingestion and transformation tests           | Both  |
| T68     | Benchmark Spark joins, partitioning and execution plans     | A     |
| T69     | Test duplicate ingestion, bad files and recovery behavior   | B     |
| T70     | Verify historical route changes against original records    | A     |
| T71     | Prepare architecture, modeling and quality documentation    | A     |
| T72     | Prepare operations, automation and deployment documentation | B     |
| T73     | Write results, limitations and engineering reflections      | Both  |
| T74     | Prepare demo and final presentation                         | Both  |

Done when: An end-to-end pipeline run can be reproduced, tested and explained, with measurable Spark performance evidence and a complete report.

# 4. Weekly task assignments

Here is the actual work allocation I would use.

| Week | Developer A                          | Developer B                   | Joint milestone                                 |
| ---- | ------------------------------------ | ----------------------------- | ----------------------------------------------- |
| 1    | T03–T05, T07–T12                     | T01–T02, T06, initial T61–T62 | Dataset scoped and sample parsing started       |
| 2    | T20–T26, finish historical ingestion | T13–T19                       | Bronze pipeline functional                      |
| 3    | T27, T29–T30, T33                    | T28, T31–T32, T35, T49–T51    | First trusted Silver tables                     |
| 4    | T34, T38–T42, T44–T45                | T36–T37, T43, T52–T53         | Silver validated and historical changes modeled |
| 5    | T46–T48, T68, T70                    | T54–T66                       | Gold and GUI deployed                           |
| 6    | T67, T71, T73–T74                    | T67, T69, T72–T74             | End-to-end tests, benchmarks and report         |

Some tasks are deliberately started before their main week—for example, CI/CD in Week 1—so that deployment doesn't become a last-minute problem.

## 5. Dependency order

EP-08 scheduling, EP-10 CI/CD and testing are cross-cutting workstreams. The GUI should start with mock data but be validated against published Gold tables.

The key coordination point is the ingestion-to-Silver contract. Developer B must write raw data and metadata in a format Developer A can consume without manual fixes. Agree on that interface in Week 1.

## 6. Recommended first sprint

I would not start with the GUI. Your first sprint should prove the source works.

## Sprint 1 — First 7 days

Developer A

- Identify the selected NeTEx snapshots.
- Use January and September 2026 samples for both KOL and AKT.
- Parse lines, journeys and journey patterns.
- Inspect real join keys and source validity dates.

Developer B

- Set up GitHub and PySpark environment.
- Build a basic ZIP ingestion script.
- Establish Bronze folder conventions and metadata.
- Verify an Entur polling endpoint and capture a sample response.

Joint checkpoint: Demonstrate a sample timetable archive flowing into Bronze and at least one structured PySpark table, and document the agreed schema.

## 7. Scope boundaries

To keep this realistic for two people, I would make these explicit:

Must deliver: Nine-month selected historical dataset for KOL and AKT, manual upload, API polling, daily/weekly cron, Bronze, substantial Silver, versioned route history, Gold, Streamlit GUI, tests, and deployment evidence.

Only if time permits: Full daily historical backfill, WebSocket streaming, Kafka, live vehicle map, and advanced automatic detection of complex route splits or mergers.

Not part of the historical analysis: Actual bus punctuality before the date you began collecting realtime observations. Historical NeTEx supports planned timetables, not historical observed delays.

## Final division

DEVELOPER A

# Data pipeline

Historical acquisition, NeTEx parsing, Silver data model, route change tracking, Gold analytics.

DEVELOPER B

# Platform pipeline

Manual ingestion, API polling, cron, Silver quality controls, GUI, CI/CD and deployment.

The responsibilities are balanced by engineering complexity, not by ticket count. Developer A has fewer but more technically demanding NeTEx and historical-modeling tasks. Developer B has more integration and infrastructure tasks.

Treat these as the initial assignments, then rebalance after Week 2 based on the actual parsing complexity and available server resources.

The non-negotiable target is a functioning Bronze → Silver → Gold pipeline by the end of Week 4. That leaves the final two weeks for the GUI, automated deployment, performance testing, and reporting—without putting the course's primary Silver-layer requirements at risk.
