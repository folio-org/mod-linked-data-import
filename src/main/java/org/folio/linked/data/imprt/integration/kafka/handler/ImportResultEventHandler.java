package org.folio.linked.data.imprt.integration.kafka.handler;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.domain.dto.ImportResultEvent;
import org.folio.linked.data.imprt.model.entity.BatchJobExecution;
import org.folio.linked.data.imprt.model.mapper.ImportResultEventMapper;
import org.folio.linked.data.imprt.repo.BatchJobExecutionParamsRepo;
import org.folio.linked.data.imprt.repo.BatchJobExecutionRepo;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.file.FileService;
import org.folio.linked.data.imprt.service.job.JobCompletionService;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class ImportResultEventHandler implements KafkaMessageHandler<ImportResultEvent> {
  private static final String FILE_URL_NOT_FOUND_MESSAGE =
    "Job parameter [fileUrl] for jobInstanceId [%s] is not found. RDF line reading failed.";

  private final FileService fileService;
  private final ImportResultEventRepo importResultEventRepo;
  private final FailedRdfLineRepo failedRdfLineRepo;
  private final ImportResultEventMapper importResultEventMapper;
  private final BatchJobExecutionParamsRepo batchJobExecutionParamsRepo;
  private final BatchJobExecutionRepo batchJobExecutionRepo;
  private final JobCompletionService jobCompletionService;

  @Override
  public void handle(ImportResultEvent importResultEvent) {
    log.debug("Handling importResultEvent {}", importResultEvent);
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
    checkJobCompletion(importResultEvent.getJobInstanceId());
  }

  private void checkJobCompletion(Long jobInstanceId) {
    var processedCount = importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId);
    var failedDuringMappingCount = failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId);
    var totalProcessedCount = processedCount + failedDuringMappingCount;
    var jobExecutionIdOpt = batchJobExecutionRepo.findFirstByJobInstanceIdOrderByJobExecutionIdDesc(jobInstanceId)
      .map(BatchJobExecution::getJobExecutionId);

    if (jobExecutionIdOpt.isPresent()) {
      jobCompletionService.checkAndCompleteJob(jobExecutionIdOpt.get(), totalProcessedCount);
    } else {
      log.warn("No active job execution found for job instance {}", jobInstanceId);
    }
  }

}
