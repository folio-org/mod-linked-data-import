# mod-linked-data-import
Copyright (C) 2025 The Open Library Foundation

This software is distributed under the terms of the Apache License, Version 2.0.
See the file "[LICENSE](LICENSE)" for more information.
## Introduction

This module provides bulk import functionality for RDF data graphs into the [`mod-linked-data`](https://github.com/folio-org/mod-linked-data) application.
It reads RDF subgraphs in Bibframe 2 format, transforms them into the Builde vocabulary, and delivers them to `mod-linked-data` via Kafka.

## Third party libraries used in this software
This software uses the following Weak Copyleft (Eclipse Public License 1.0 / 2.0) licensed software libraries:

- [jakarta.annotation-api](https://projects.eclipse.org/projects/ee4j.ca)
- [jakarta.json-api](https://github.com/jakartaee/jsonp-api)
- [junit](https://junit.org/)
- [aspectjweaver](https://eclipse.dev/aspectj/)

## How to Import Data
1. Upload the RDF file to the S3 bucket specified by the `S3_BUCKET` environment variable.
2. Inside that bucket, place the file within the subdirectory corresponding to the target tenant ID.
3. Trigger the import by calling the following API:
```
POST /linked-data-import/start?fileUrl={fileNameInS3}&contentType=application/ld+json
x-okapi-tenant: {tenantId}
x-okapi-token: {token}
```
Response is a job id, which could be later used for getting job status or failed lines.
## To check the import job status, use:
```
GET /linked-data-import/jobs/{jobExecutionId}
x-okapi-tenant: {tenantId}
x-okapi-token: {token}
```
The response includes job information such as:
- `startDate`: Job start date and time
- `startedBy`: User ID who started the job
- `status`: Current job status (COMPLETED, STARTED, FAILED, etc.)
- `fileName`: Name of the imported file
- `currentStep`: Current processing step
- `linesRead`: Total lines read from the file
- `linesMapped`: Lines successfully mapped
- `linesFailedMapping`: Lines failed during mapping
- `linesCreated`: Resources created
- `linesUpdated`: Resources updated
- `linesFailedSaving`: Lines failed during saving

## To download failed RDF lines as CSV file:
```
GET /linked-data-import/jobs/{jobExecutionId}/failed-lines
x-okapi-tenant: {tenantId}
x-okapi-token: {token}
```
The CSV file contains:
- `lineNumber`: Line number in the original file
- `description`: Error description
- `failedRdfLine`: The RDF line content that failed

## To cancel a running import job:
```
PUT /linked-data-import/jobs/{jobExecutionId}/cancel
x-okapi-tenant: {tenantId}
x-okapi-token: {token}
```
**Note:** The job will stop gracefully after completing the current processing chunk or step. It will not stop immediately.

## File Format & Contents

1. The file must be in **JSON Lines (jsonl)** format.
2. Each line must contain a complete subgraph of a **Bibframe Instance** resource, as defined by the [Bibframe 2 ontology](https://id.loc.gov/ontologies/bibframe.html).

For an example of a valid import file containing two RDF instances, see [docs/example-import.jsonl](./docs/example-import.jsonl).

## Limitations
1. Only RDF data serialized as `application/ld+json` is supported.
   Support for additional formats (e.g., XML, N-Triples) may be added in the future.
2. Only **Bibframe Instances** and their connected resources can be imported.
   Standalone resources—such as a [Person](https://id.loc.gov/ontologies/bibframe.html#c_Person) not linked to any Instance—cannot be processed.

## Batch processing
File contents are processed in batches.
You can configure batch processing using following environment variables:
1. CHUNK_SIZE: Number of lines read from the input file per chunk
2. OUTPUT_CHUNK_SIZE: Number of Graph resources sent to Kafka per chunk
3. PROCESS_FILE_MAX_POOL_SIZE: Maximum threads used for parallel chunk processing

## Interaction with mod-linked-data

`mod-linked-data` uses the [Builde vocabulary](https://bibfra.me/) for representing graph data.

During import:

1. This module transforms Bibframe 2 subgraphs into the equivalent Builde subgraph using the [`lib-linked-data-rdf4ld`](https://github.com/folio-org/lib-linked-data-rdf4ld) library.
2. The transformed subgraphs are published to the Kafka topic specified by the `KAFKA_LINKED_DATA_IMPORT_OUTPUT_TOPIC` environment variable.
3. `mod-linked-data` consumes messages from this topic, performs additional processing, and persists the graph to its database.

## Dependencies on libraries
This module is dependent on the following libraries:
- [lib-linked-data-dictionary](https://github.com/folio-org/lib-linked-data-dictionary)
- [lib-linked-data-fingerprint](https://github.com/folio-org/lib-linked-data-fingerprint)
- [lib-linked-data-rdf4ld](https://github.com/folio-org/lib-linked-data-rdf4ld)
## Compiling
```bash
mvn clean install
```
Skip tests:
```bash
mvn clean install -DskipTests
```

### Environment variables
This module uses S3 storage for files. AWS S3 and Minio Server are supported for files storage.
It is also necessary to specify variable S3_IS_AWS to determine if AWS S3 is used as files storage. By default,
this variable is `false` and means that MinIO server is used as storage.
This value should be `true` if AWS S3 is used.

| Name                                                     | Default value             | Description                                                                 |
|:---------------------------------------------------------|:--------------------------|:----------------------------------------------------------------------------|
| SERVER_PORT                                              | 8081                      | Server port                                                                 |
| DB_USERNAME                                              | postgres                  | Database username                                                           |
| DB_PASSWORD                                              | postgres                  | Database password                                                           |
| DB_HOST                                                  | postgres                  | Database host                                                               |
| DB_PORT                                                  | 5432                      | Database port                                                               |
| DB_DATABASE                                              | okapi_modules             | Database name                                                               |
| KAFKA_HOST                                               | kafka                     | Kafka broker host                                                           |
| KAFKA_PORT                                               | 9092                      | Kafka broker port                                                           |
| KAFKA_CONSUMER_MAX_POLL_RECORDS                          | 200                       | Maximum number of records returned in a single poll                         |
| KAFKA_SECURITY_PROTOCOL                                  | PLAINTEXT                 | Kafka security protocol                                                     |
| KAFKA_SSL_KEYSTORE_PASSWORD                              | -                         | Kafka SSL keystore password                                                 |
| KAFKA_SSL_KEYSTORE_LOCATION                              | -                         | Kafka SSL keystore location                                                 |
| KAFKA_SSL_TRUSTSTORE_PASSWORD                            | -                         | Kafka SSL truststore password                                               |
| KAFKA_SSL_TRUSTSTORE_LOCATION                            | -                         | Kafka SSL truststore location                                               |
| ENV                                                      | folio                     | Environment name used in Kafka topic names                                  |
| KAFKA_RETRY_INTERVAL_MS                                  | 2000                      | Kafka retry interval in milliseconds                                        |
| KAFKA_RETRY_DELIVERY_ATTEMPTS                            | 6                         | Number of Kafka delivery retry attempts                                     |
| KAFKA_IMPORT_RESULT_EVENT_CONCURRENCY                    | 1                         | Number of concurrent consumers for import result events                     |
| KAFKA_IMPORT_RESULT_EVENT_TOPIC_PATTERN                  | (${ENV}\.)(.*\.)result    | Kafka topic pattern for import result events                                |
| KAFKA_LINKED_DATA_IMPORT_OUTPUT_TOPIC                    | linked_data_import.output | Kafka topic where the transformed subgraph is published for mod-linked-data |
| KAFKA_LINKED_DATA_IMPORT_OUTPUT_TOPIC_PARTITIONS         | 3                         | Number of partitions for the output topic                                   |
| KAFKA_LINKED_DATA_IMPORT_OUTPUT_TOPIC_REPLICATION_FACTOR | -                         | Replication factor for the output topic                                     |
| KAFKA_LINKED_DATA_IMPORT_RESULT_TOPIC                    | linked_data_import.result | Kafka topic for import processing results                                   |
| KAFKA_LINKED_DATA_IMPORT_RESULT_TOPIC_PARTITIONS         | 3                         | Number of partitions for the result topic                                   |
| KAFKA_LINKED_DATA_IMPORT_RESULT_TOPIC_REPLICATION_FACTOR | -                         | Replication factor for the result topic                                     |
| S3_URL                                                   | http://127.0.0.1:9000/    | S3 url                                                                      |
| S3_REGION                                                | -                         | S3 region                                                                   |
| S3_BUCKET                                                | -                         | S3 bucket                                                                   |
| S3_ACCESS_KEY_ID                                         | -                         | S3 access key                                                               |
| S3_SECRET_ACCESS_KEY                                     | -                         | S3 secret key                                                               |
| S3_IS_AWS                                                | false                     | Specify if AWS S3 is used as files storage                                  |
| CHUNK_SIZE                                               | 1000                      | Number of lines read from the input file per chunk                          |
| OUTPUT_CHUNK_SIZE                                        | 100                       | Number of Graph resources sent to Kafka per chunk                           |
| JOB_POOL_SIZE                                            | 1                         | Number of concurrent Import Jobs                                            |
| PROCESS_FILE_MAX_POOL_SIZE                               | 1000                      | Maximum threads used for parallel chunk processing                          |
| WAIT_FOR_PROCESSING_INTERVAL_MS                          | 5000                      | Interval in milliseconds to wait between checks for processing completion   |
