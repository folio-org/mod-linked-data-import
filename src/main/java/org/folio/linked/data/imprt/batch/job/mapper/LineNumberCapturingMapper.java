package org.folio.linked.data.imprt.batch.job.mapper;

import org.folio.linked.data.imprt.model.RdfLineWithNumber;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.item.file.LineMapper;

public class LineNumberCapturingMapper implements LineMapper<RdfLineWithNumber> {

  @Override
  public @NotNull RdfLineWithNumber mapLine(@NotNull String line, int lineNumber) {
    return new RdfLineWithNumber(lineNumber, line);
  }
}

