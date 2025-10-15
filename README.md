# mod-linked-data-import
Copyright (C) 2025 The Open Library Foundation

This software is distributed under the terms of the Apache License, Version 2.0.
See the file "[LICENSE](LICENSE)" for more information.
## Introduction
mod-linked-data-import manages data import process to Linked Data graph in FOLIO.
### Dependencies on libraries
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

| Name                       | Default value          | Description                                    |
|:---------------------------|:-----------------------|:-----------------------------------------------|
| S3_URL                     | http://127.0.0.1:9000/ | S3 url                                         |
| S3_REGION                  | -                      | S3 region                                      |
| S3_BUCKET                  | -                      | S3 bucket                                      |
| S3_ACCESS_KEY_ID           | -                      | S3 access key                                  |
| S3_SECRET_ACCESS_KEY       | -                      | S3 secret key                                  |
| S3_IS_AWS                  | false                  | Specify if AWS S3 is used as files storage     |
| CHUNK_SIZE                 | 1000                   | Chunk size (lines number) for processing file  |
| OUTPUT_CHUNK_SIZE          | 1000                   | Output chunk size for Kafka publishing         |
| PROCESS_FILE_MAX_POOL_SIZE | 1000                   | Max threads for parallel processFile step      |
