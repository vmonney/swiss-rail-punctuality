# Evidence App (Phase 8)

This folder contains the Phase 8 BI report implemented with Evidence.

## Prerequisites

- Node.js 20+
- npm
- Access to BigQuery marts
- `gcloud auth application-default login` for local auth

## Configure

```bash
cp .env.example .env
```

Set:

- `EVIDENCE_BQ_PROJECT`
- `EVIDENCE_BQ_DATASET` (default: `sbb_punctuality_marts`)

## Run

```bash
npm install
npm run sources
npm run dev
```

## Build

```bash
npm run build
npm run preview
```
