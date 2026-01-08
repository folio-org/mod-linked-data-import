package org.folio.linked.data.imprt.batch.job.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.service.job.JobService;
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

  private final JobService jobService;

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

    var mappedCount = jobService.getMappedCount(jobExecutionId);
    if (mappedCount == 0) {
      log.warn("No lines were mapped for job execution {}", jobExecutionId);
      return RepeatStatus.FINISHED;
    }

    var totalSavedCount = jobService.getSavedCount(jobExecutionId);

    if (totalSavedCount >= mappedCount) {
      log.info("Processing completed for job execution {}. Mapped: {}, Saved: {}",
        jobExecutionId, mappedCount, totalSavedCount);
      return RepeatStatus.FINISHED;
    }

    log.debug("Processing not yet completed for job execution {}. Mapped: {}, Saved: {}. Waiting...",
      jobExecutionId, mappedCount, totalSavedCount);

    Thread.sleep(waitIntervalMs);
    return RepeatStatus.CONTINUABLE;
  }


}

