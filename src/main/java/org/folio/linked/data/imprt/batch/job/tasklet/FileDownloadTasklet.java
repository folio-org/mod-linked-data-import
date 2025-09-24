package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;

import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class FileDownloadTasklet implements Tasklet {

  @Override
  public RepeatStatus execute(@NotNull StepContribution contribution, @NotNull ChunkContext chunkContext) {
    var fileName = chunkContext.getStepContext()
      .getStepExecution()
      .getJobParameters()
      .getString(FILE_URL);
    log.info("Starting file download {}", fileName);

    return RepeatStatus.FINISHED;
  }

}
