package org.folio.linked.data.imprt.batch.job.tasklet;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Log4j2
@Component
@RequiredArgsConstructor
public class FileCleanupTasklet implements Tasklet {
  private final RdfFileLineRepo rdfFileLineRepo;

  @Override
  @Transactional
  public RepeatStatus execute(@NotNull StepContribution contribution, @NotNull ChunkContext chunkContext) {
    var jobExecutionId = chunkContext.getStepContext().getStepExecution().getJobExecutionId();
    rdfFileLineRepo.deleteByJobExecutionId(jobExecutionId);
    log.info("Deleted RDF file lines for job execution {}", jobExecutionId);
    return RepeatStatus.FINISHED;
  }

}

