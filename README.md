# mod-linked-data-import
Copyright (C) 2025 The Open Library Foundation

This software is distributed under the terms of the Apache License, Version 2.0.
See the file "[LICENSE](LICENSE)" for more information.
## Introduction

This module provides bulk import functionality for RDF data graphs into the [`mod-linked-data`](https://github.com/folio-org/mod-linked-data) application.
It reads RDF subgraphs in Bibframe 2 format, transforms them into the Builde vocabulary, and delivers them to `mod-linked-data` via Kafka.

## How to Import Data
1. Upload the RDF file to the S3 bucket specified by the `S3_BUCKET` environment variable.
2. Inside that bucket, place the file within the subdirectory corresponding to the target tenant ID.
3. Trigger the import by calling the following API:
```
POST /linked-data-import/start?fileUrl={fileNameInS3}&contentType=application/ld+json
x-okapi-tenant: {tenantId}
x-okapi-token: {token}
```

## File Format Requirements

1. The file must be in **JSON Lines (jsonl)** format.
2. Each line must contain a complete subgraph of a **Bibframe Instance** resource, as defined by the [Bibframe 2 ontology](https://id.loc.gov/ontologies/bibframe.html).

## Limitations
1. Only RDF data serialized as `application/ld+json` is supported.
   Support for additional formats (e.g., XML, N-Triples) may be added in the future.
2. Only **Bibframe Instances** and their connected resources can be imported.
   Standalone resources—such as a [Person](https://id.loc.gov/ontologies/bibframe.html#c_Person) not linked to any Instance—cannot be processed.

## Batch processing
File contents are processed in batches.
You can configure the batch size using the `CHUNK_SIZE` environment variable.

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

| Name                                  | Default value             | Description                                                                 |
|:--------------------------------------|:--------------------------|:----------------------------------------------------------------------------|
| S3_URL                                | http://127.0.0.1:9000/    | S3 url                                                                      |
| S3_REGION                             | -                         | S3 region                                                                   |
| S3_BUCKET                             | -                         | S3 bucket                                                                   |
| S3_ACCESS_KEY_ID                      | -                         | S3 access key                                                               |
| S3_SECRET_ACCESS_KEY                  | -                         | S3 secret key                                                               |
| S3_IS_AWS                             | false                     | Specify if AWS S3 is used as files storage                                  |
| CHUNK_SIZE                            | 1000                      | Chunk size (lines number) for processing file                               |
| OUTPUT_CHUNK_SIZE                     | 1000                      | Output chunk size for Kafka publishing                                      |
| PROCESS_FILE_MAX_POOL_SIZE            | 1000                      | Max threads for parallel processFile step                                   |
| KAFKA_LINKED_DATA_IMPORT_OUTPUT_TOPIC | linked_data_import.output | Kafka topic where the transformed subgraph is published for mod-linked-data |
