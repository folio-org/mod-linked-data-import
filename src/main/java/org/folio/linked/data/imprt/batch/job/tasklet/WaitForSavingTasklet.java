package org.folio.linked.data.imprt.batch.job.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.repo.BatchStepExecutionRepo;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class WaitForSavingTasklet implements Tasklet {

  private final BatchStepExecutionRepo batchStepExecutionRepo;
  private final ImportResultEventRepo importResultEventRepo;
  private final FailedRdfLineRepo failedRdfLineRepo;

  @Value("${mod-linked-data-import.wait-for-processing-interval-ms}")
  private int waitIntervalMs;

  @Override
  public RepeatStatus execute(@NotNull StepContribution contribution, @NotNull ChunkContext chunkContext)
    throws InterruptedException {
    var jobInstanceId = chunkContext.getStepContext()
      .getStepExecution()
      .getJobExecution()
      .getJobInstance()
      .getInstanceId();

    var totalReadCount = batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobInstanceId);
    if (totalReadCount == 0) {
      log.warn("No lines read for job instance {}", jobInstanceId);
      return RepeatStatus.FINISHED;
    }

    var processedCount = importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId);
    var failedDuringMappingCount = failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId);
    var totalProcessedCount = processedCount + failedDuringMappingCount;

    if (totalProcessedCount >= totalReadCount) {
      log.info("Processing completed for job instance {}. Read: {}, Processed: {} (Successful: {}, "
          + "Failed during mapping: {})", jobInstanceId, totalReadCount, totalProcessedCount, processedCount,
        failedDuringMappingCount);
      return RepeatStatus.FINISHED;
    }

    log.debug("Processing not yet completed for job instance {}. Read: {}, Processed: {} (Successful: {}, "
        + "Failed during mapping: {}). Waiting...", jobInstanceId, totalReadCount, totalProcessedCount, processedCount,
      failedDuringMappingCount);

    Thread.sleep(waitIntervalMs);
    return RepeatStatus.CONTINUABLE;
  }

}

