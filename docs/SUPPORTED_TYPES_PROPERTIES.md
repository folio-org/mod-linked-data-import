# Supported Bibframe Types & properties

## Introduction

This document lists the Bibframe types and properties currently supported by the `mod-linked-data-import` module.
It details the RDF properties, their expected value types, and the relationships between resources as handled by the import process.

This page will be updated as support for additional Bibframe types and properties is added to the module.

## Type: [Instance](http://id.loc.gov/ontologies/bibframe/Instance)

| Property                                                      | Range                                                                                                                                                                                                                                                                                                                                                    |
|:--------------------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| http://id.loc.gov/ontologies/bibframe/title                   | [Title](http://id.loc.gov/ontologies/bibframe/Title), [ParallelTitle](http://id.loc.gov/ontologies/bibframe/ParallelTitle), [VariantTitle](http://id.loc.gov/ontologies/bibframe/VariantTitle)                                                                                                                                                           |
| http://id.loc.gov/ontologies/bibframe/identifiedBy            | [Isbn](http://id.loc.gov/ontologies/bibframe/Isbn), [Lccn](http://id.loc.gov/ontologies/bibframe/Lccn), [Ean](http://id.loc.gov/ontologies/bibframe/Ean)                                                                                                                                                                                                 |
| http://id.loc.gov/ontologies/bibframe/provisionActivity       | [ProvisionActivity](http://id.loc.gov/ontologies/bibframe/ProvisionActivity) + [Distribution](http://id.loc.gov/ontologies/bibframe/Distribution) / [Manufacture](http://id.loc.gov/ontologies/bibframe/Manufacture) / [Production](http://id.loc.gov/ontologies/bibframe/Production) / [Publication](http://id.loc.gov/ontologies/bibframe/Publication) |
| http://id.loc.gov/ontologies/bibframe/instanceOf              | [Work](http://id.loc.gov/ontologies/bibframe/Work)                                                                                                                                                                                                                                                                                                       |
| http://id.loc.gov/ontologies/bibframe/dimensions              | literal                                                                                                                                                                                                                                                                                                                                                  |
| http://id.loc.gov/ontologies/bibframe/responsibilityStatement | literal                                                                                                                                                                                                                                                                                                                                                  |

## Type: [Work](http://id.loc.gov/ontologies/bibframe/Work)
| Property                                                  | Range                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|:----------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| http://id.loc.gov/ontologies/bibframe/title               | [Title](http://id.loc.gov/ontologies/bibframe/Title), [ParallelTitle](http://id.loc.gov/ontologies/bibframe/ParallelTitle), [VariantTitle](http://id.loc.gov/ontologies/bibframe/VariantTitle)                                                                                                                                                                                                                                                                         |
| http://id.loc.gov/ontologies/bibframe/contribution        | Contribution, PrimaryContribution                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| http://id.loc.gov/ontologies/bibframe/subject             | Either a URI referencing a valid LCCN, or a blank node pointing to a [Topic](http://id.loc.gov/ontologies/bibframe/Topic), [Person](http://id.loc.gov/ontologies/bibframe/Person), [Family](http://id.loc.gov/ontologies/bibframe/Family), [Organization](http://id.loc.gov/ontologies/bibframe/Organization), [Meeting](http://id.loc.gov/ontologies/bibframe/Meeting), [Jurisdiction](http://id.loc.gov/ontologies/bibframe/Jurisdiction), containing an rdfs:label. |

## Types: [Contribution](http://id.loc.gov/ontologies/bibframe/Contribution) and [PrimaryContribution](http://id.loc.gov/ontologies/bibframe/PrimaryContribution)
| Property                                                  | Range                                                                            |
|:----------------------------------------------------------|:---------------------------------------------------------------------------------|
| http://id.loc.gov/ontologies/bibframe/agent               | Either a URI referencing a valid LCCN, or a blank node containing an rdfs:label. |
| http://id.loc.gov/ontologies/bibframe/role                | A URI from the [relator vocabulary](http://id.loc.gov/vocabulary/relators)       |


## Types: [Isbn](http://id.loc.gov/ontologies/bibframe/Isbn), [Lccn](http://id.loc.gov/ontologies/bibframe/Lccn), [Ean](http://id.loc.gov/ontologies/bibframe/Ean)
| Property                                         | Range                                                                 |
|:-------------------------------------------------|:----------------------------------------------------------------------|
| http://www.w3.org/1999/02/22-rdf-syntax-ns#value | literal                                                               |
| http://id.loc.gov/ontologies/bibframe/qualifier  | literal                                                               |
| http://id.loc.gov/ontologies/bibframe/status     | A URI from [mstatus vocabulary](http://id.loc.gov/vocabulary/mstatus) |

## Type: [Title](http://id.loc.gov/ontologies/bibframe/Title)
| Property                                         | Range   |
|:-------------------------------------------------|:--------|
| http://id.loc.gov/ontologies/bibframe/mainTitle  | literal |
| http://id.loc.gov/ontologies/bibframe/partName   | literal |
| http://id.loc.gov/ontologies/bibframe/partNumber | literal |
| "http://id.loc.gov/ontologies/bibframe/subtitle  | literal |
| http://id.loc.gov/ontologies/bflc/nonSortNum     | literal |

## Type: [ParallelTitle](http://id.loc.gov/ontologies/bibframe/ParallelTitle)
| Property                                         | Range                                              |
|:-------------------------------------------------|:---------------------------------------------------|
| http://id.loc.gov/ontologies/bibframe/mainTitle  | literal                                            |
| http://id.loc.gov/ontologies/bibframe/partName   | literal                                            |
| http://id.loc.gov/ontologies/bibframe/partNumber | literal                                            |
| http://id.loc.gov/ontologies/bibframe/subtitle   | literal                                            |
| http://id.loc.gov/ontologies/bflc/nonSortNum     | literal                                            |
| http://id.loc.gov/ontologies/bibframe/date       | literal                                            |
| http://id.loc.gov/ontologies/bibframe/note       | [Note](http://id.loc.gov/ontologies/bibframe/Note) |

## Type: [VariantTitle](http://id.loc.gov/ontologies/bibframe/VariantTitle)
| Property                                          | Range                                              |
|:--------------------------------------------------|:---------------------------------------------------|
| http://id.loc.gov/ontologies/bibframe/mainTitle   | literal                                            |
| http://id.loc.gov/ontologies/bibframe/partName    | literal                                            |
| http://id.loc.gov/ontologies/bibframe/partNumber  | literal                                            |
| "http://id.loc.gov/ontologies/bibframe/subtitle   | literal                                            |
| http://id.loc.gov/ontologies/bflc/nonSortNum      | literal                                            |
| http://id.loc.gov/ontologies/bibframe/date        | literal                                            |
| http://id.loc.gov/ontologies/bibframe/note        | [Note](http://id.loc.gov/ontologies/bibframe/Note) |
| http://id.loc.gov/ontologies/bibframe/variantType | literal                                            |

## Types: [ProvisionActivity](http://id.loc.gov/ontologies/bibframe/ProvisionActivity) + [Distribution](http://id.loc.gov/ontologies/bibframe/Distribution) / [Manufacture](http://id.loc.gov/ontologies/bibframe/Manufacture) / [Production](http://id.loc.gov/ontologies/bibframe/Production) / [Publication](http://id.loc.gov/ontologies/bibframe/Publication)
| Property                                      | Range                                           |
|:----------------------------------------------|:------------------------------------------------|
| http://id.loc.gov/ontologies/bibframe/date    | literal                                         |
| http://id.loc.gov/ontologies/bflc/simpleDate  | literal                                         |
| http://id.loc.gov/ontologies/bflc/simpleAgent | literal                                         |
| http://id.loc.gov/ontologies/bflc/simplePlace | literal                                         |
| http://id.loc.gov/ontologies/bibframe/place   | URI from http://id.loc.gov/vocabulary/countries |

## Example Import File
For an example of a valid import file containing two RDF instances, see [example-import.jsonl](./example-import.jsonl).
