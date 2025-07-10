# Stream Firestore to BigQuery

This project contains Google Cloud Functions that streams data from Firestore to BigQuery in real-time. It also includes utility functions to manage tables and table schemas in BigQuery using command lines.

## Features

- Real-time streaming of Firestore changes to a buffer tables in BigQuery. Nested objects and arrays are flattened for easier querying. During this process:
  - Warning messages are saved in a warning table for records have schema issue compared with pre-defined schema.
  - Error messages are saved in a error table for records that cannot be successfully processed.
  - Other logic (data cleanup, data transormation, etc) can be added in this process
- At defined intervals (e.g. every 30 minutes), buffer tables are synchronized into target tables (the tables to be used for downstream analysis). The data sync is scheduled using Cloud Scheduler, but can also be triggered manually for flexibility.
- `localRun.js` can run directly, or accept arguments from command line, to use local utility functions for manual managing Firestore and BigQuery data.

## Prerequisites

1. A Google Cloud project with Firestore and BigQuery enabled.
2. Node.js and npm installed locally.
3. The `gcloud` CLI installed and authenticated.

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/episphere/stream-Firestore-to-BigQuery.git
cd stream-Firestore-to-BigQuery
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Deploy the Cloud Function

#### Adjust settings in `settings.js` file

Check the `settings.js` file for any necessary configurations, such as target dataset name, buffer dataset name, error and warning table names, collection names to be tracked.

#### Adjust table schemas in `tableSchemas.js` file

Check the `tableSchemas.js` file for the schemas of the target tables. Adjust the schemas as needed.

#### Create tables in BigQuery

Create buffer tables for an environment (e.g. dev, prod). Defined dataset name and table schemas are used in this step.

```bash
node localRun.js --entry createAllBufferTables --gcloud --env dev
```

Create target tables for an environment (e.g. dev, prod)

```bash
node localRun.js --entry createAllTargetTables --gcloud --env dev
```

Create error and warning tables for an environment (e.g. dev, prod)

```bash
node localRun.js --entry createLogTables --gcloud --env dev
```

#### Deploy function triggered by Firestore write events

```bash
gcloud functions deploy stream-firestore-updates \
--source=. \
--gen2 \
--runtime=nodejs22 \
--entry-point=streamFirestoreUpdates \
--ingress-settings=internal-only \
--region=us-central1 \
--trigger-location=nam5 \
--trigger-event-filters=type=google.cloud.firestore.document.v1.written \
--trigger-event-filters=database='(default)' \
--memory=1Gi \
--cpu=1 \
--timeout=300s \
--concurrency=80
```

The `stream-firestore-updates` function is triggered by Firestore write events. It streams the changes to buffer dataset (default name `firstore_stream_buffer`) in BigQuery.

#### Deploy function triggered by HTTP requests

```bash
gcloud functions deploy sync-batched-updates-to-tables \
--gen2 \
--trigger-http \
--region=us-central1 \
--runtime=nodejs22 \
--source=. \
--entry-point=syncBatchedUpdatesToTables \
--ingress-settings=internal-only
```

The `sync-batched-updates-to-tables` function is responsible for merging the buffered data into the target tables in dataset (default name `firestore_streram`).
HTTP requests to this function can be scheduled using Cloud Scheduler or triggered manually.
