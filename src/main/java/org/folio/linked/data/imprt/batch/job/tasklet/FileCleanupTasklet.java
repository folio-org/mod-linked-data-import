package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;
import static org.folio.linked.data.imprt.util.FileUtil.extractFileName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class FileCleanupTasklet implements Tasklet {

  @Override
  public RepeatStatus execute(@NotNull StepContribution contribution, @NotNull ChunkContext chunkContext) {
    var fileUrl = chunkContext.getStepContext()
      .getStepExecution()
      .getJobParameters()
      .getString(FILE_URL);
    var file = new File(TMP_DIR, extractFileName(fileUrl));
    try {
      var deleted = Files.deleteIfExists(file.toPath());
      if (deleted) {
        log.info("Temporary file {} deleted", file.getAbsolutePath());
      }
    } catch (IOException e) {
      log.warn("Failed to delete temporary file {}: {}", file.getAbsolutePath(), e.getMessage());
    }
    return RepeatStatus.FINISHED;
  }

}

