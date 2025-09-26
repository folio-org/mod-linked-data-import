package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;

import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.service.s3.S3Service;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class FileDownloadTasklet implements Tasklet {
  private final S3Service s3Service;

  @Override
  public RepeatStatus execute(@NotNull StepContribution contribution, @NotNull ChunkContext chunkContext) {
    var fileUrl = chunkContext.getStepContext()
      .getStepExecution()
      .getJobParameters()
      .getString(FILE_URL);
    try {
      s3Service.download(fileUrl, TMP_DIR);
    } catch (IOException e) {
      log.error("Error downloading file {}", fileUrl, e);
      throw new IllegalArgumentException("Error downloading file " + fileUrl, e);
    }
    return RepeatStatus.FINISHED;
  }

}
