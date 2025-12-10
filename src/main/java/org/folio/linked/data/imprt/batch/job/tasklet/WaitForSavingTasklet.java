package org.folio.linked.data.imprt.batch.job.tasklet;

import static java.util.concurrent.TimeUnit.MINUTES;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.repo.BatchStepExecutionRepo;
import org.folio.linked.data.imprt.service.job.JobCompletionService;
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
  private final JobCompletionService jobCompletionService;

  @Value("${mod-linked-data-import.wait-for-processing-lines-per-minute:500}")
  private int linesPerMinute;

  @Override
  public RepeatStatus execute(@NotNull StepContribution contribution, @NotNull ChunkContext chunkContext)
    throws InterruptedException {
    var jobExecutionId = chunkContext.getStepContext()
      .getStepExecution()
      .getJobExecution()
      .getId();

    var totalReadCount = batchStepExecutionRepo.getTotalReadCountByJobExecutionId(jobExecutionId);
    if (totalReadCount == 0) {
      log.info("No lines read for job execution {}", jobExecutionId);
      return RepeatStatus.FINISHED;
    }

    var timeout = calculateTimeout(totalReadCount);
    log.info("Waiting for job execution {} completion. Total lines to process: {}, calculated timeout: {} minutes",
      jobExecutionId, totalReadCount, timeout);

    var completed = jobCompletionService.awaitCompletion(jobExecutionId, totalReadCount, timeout, MINUTES);

    if (completed) {
      log.info("Processing completed for job execution {}", jobExecutionId);
    } else {
      log.error("Timeout waiting for job execution {} to complete after {} minutes", jobExecutionId, timeout);
    }

    return RepeatStatus.FINISHED;
  }

  private long calculateTimeout(Long totalReadCount) {
    var calculatedMinutes = (long) Math.ceil((double) totalReadCount / linesPerMinute);
    return Math.max(calculatedMinutes, 1);
  }


}

