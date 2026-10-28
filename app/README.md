# Streamlit interface — planned

The GUI will live here: manual ZIP uploads, provider/date/route filters,
snapshot comparisons, and ingestion/quality status.

The application has not been implemented yet. Shared pipeline logic belongs in
`pipeline/`; the app will call that logic and read published analytical data.

The upload backend is now available as
`pipeline.ingestion.ingest_local_zip.ingest_zip`. It accepts an existing ZIP,
an explicit source date and a new Bronze output directory. It preserves original
bytes, validates the selected providers and detects repeated manual uploads.
See [manual ingestion](../docs/manual_ingestion.md) before wiring up the GUI.
