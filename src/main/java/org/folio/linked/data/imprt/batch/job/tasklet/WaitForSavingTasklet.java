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
  private final FailedRdfLineRepo failedRdfLineRepo;
  private final ImportResultEventRepo importResultEventRepo;

  @Value("${mod-linked-data-import.wait-for-processing-interval-ms}")
  private int waitIntervalMs;

  @Override
  public RepeatStatus execute(@NotNull StepContribution contribution, @NotNull ChunkContext chunkContext)
    throws InterruptedException {
    var jobExecution = chunkContext.getStepContext()
      .getStepExecution()
      .getJobExecution();
    var jobExecutionId = jobExecution.getId();

    if (jobExecution.isStopping()) {
      log.info("Job execution {} is stopping, exiting wait tasklet", jobExecutionId);
      return RepeatStatus.FINISHED;
    }

    var totalReadCount = batchStepExecutionRepo.getTotalReadCountByJobExecutionId(jobExecutionId);
    if (totalReadCount == 0) {
      log.warn("No lines read for job execution {}", jobExecutionId);
      return RepeatStatus.FINISHED;
    }

    var writeCount = batchStepExecutionRepo.getTotalWriteCountByJobExecutionId(jobExecutionId);
    var failedDuringMappingCount = failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobExecutionId);
    var totalMappedCount = writeCount + failedDuringMappingCount;

    var importResultEvents = importResultEventRepo.findAllByJobExecutionId(jobExecutionId);
    var savedFromImportResults = importResultEvents.stream()
      .mapToLong(event -> event.getCreatedCount() + event.getUpdatedCount() + event.getFailedRdfLines().size())
      .sum();
    var totalSavedCount = savedFromImportResults + failedDuringMappingCount;

    if (totalMappedCount >= totalReadCount && totalSavedCount >= totalReadCount) {
      log.info("Processing completed for job execution {}. Read: {}, Mapped: {}, Saved: {}",
        jobExecutionId, totalReadCount, totalMappedCount, totalSavedCount);
      return RepeatStatus.FINISHED;
    }

    log.debug("Processing not yet completed for job execution {}. Read: {}, Mapped: {}, Saved: {}. Waiting...",
      jobExecutionId, totalReadCount, totalMappedCount, totalSavedCount);

    Thread.sleep(waitIntervalMs);
    return RepeatStatus.CONTINUABLE;
  }


}

