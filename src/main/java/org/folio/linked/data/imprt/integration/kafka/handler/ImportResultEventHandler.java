package org.folio.linked.data.imprt.integration.kafka.handler;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.linked.data.imprt.model.mapper.ImportResultEventMapper;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.rdfline.RdfLineService;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class ImportResultEventHandler implements KafkaMessageHandler<ImportResultEvent> {

  private final RdfLineService rdfLineService;
  private final ImportResultEventRepo importResultEventRepo;
  private final ImportResultEventMapper importResultEventMapper;

  @Override
  public void handle(ImportResultEvent importResultEvent) {
    var entity = importResultEventMapper.toEntity(importResultEvent);
    if (!entity.getFailedRdfLines().isEmpty()) {
      var jobExecutionId = importResultEvent.getJobExecutionId();
      entity.getFailedRdfLines().forEach(failedLine ->
        failedLine.setFailedRdfLine(rdfLineService.readLineContent(jobExecutionId, failedLine.getLineNumber()))
      );
    }
    importResultEventRepo.save(entity);
  }

}
