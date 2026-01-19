package org.folio.linked.data.imprt.service.rdfline;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.model.entity.RdfFileLine;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class RdfLineServiceImpl implements RdfLineService {

  private static final String LINE_NOT_FOUND =
    "Line number %s not found in database for jobExecutionId %s";

  private final RdfFileLineRepo rdfFileLineRepo;

  @Override
  public String readLineContent(Long jobExecutionId, Long lineNumber) {
    return rdfFileLineRepo.findByJobExecutionIdAndLineNumber(jobExecutionId, lineNumber)
      .map(RdfFileLine::getContent)
      .orElseGet(() -> {
        var notFound = LINE_NOT_FOUND.formatted(lineNumber, jobExecutionId);
        log.warn(notFound);
        return notFound;
      });
  }
}

