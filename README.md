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
