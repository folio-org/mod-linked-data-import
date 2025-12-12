package org.folio.linked.data.imprt.integration.kafka.handler;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.linked.data.imprt.model.mapper.ImportResultEventMapper;
import org.folio.linked.data.imprt.repo.BatchJobExecutionParamsRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.file.FileService;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class ImportResultEventHandler implements KafkaMessageHandler<ImportResultEvent> {
  private static final String FILE_URL_NOT_FOUND_MESSAGE =
    "Job parameter [fileUrl] for jobInstanceId [%s] is not found. RDF line reading failed.";

  private final FileService fileService;
  private final ImportResultEventRepo importResultEventRepo;
  private final ImportResultEventMapper importResultEventMapper;
  private final BatchJobExecutionParamsRepo batchJobExecutionParamsRepo;

  @Override
  public void handle(ImportResultEvent importResultEvent) {
    var entity = importResultEventMapper.toEntity(importResultEvent);
    if (!entity.getFailedRdfLines().isEmpty()) {
      var jobInstanceId = importResultEvent.getJobInstanceId();
      var fileUrl = batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobInstanceId, FILE_URL);
      entity.getFailedRdfLines().forEach(failedLine ->
        failedLine.setFailedRdfLine(fileUrl.map(url -> fileService.readLineFromFile(url, failedLine.getLineNumber()))
          .orElse(FILE_URL_NOT_FOUND_MESSAGE.formatted(jobInstanceId))
        )
      );
    }
    importResultEventRepo.save(entity);
  }

}
