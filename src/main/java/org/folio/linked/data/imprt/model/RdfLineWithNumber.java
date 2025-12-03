package org.folio.linked.data.imprt.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RdfLineWithNumber {

  private long lineNumber;
  private String content;

}
