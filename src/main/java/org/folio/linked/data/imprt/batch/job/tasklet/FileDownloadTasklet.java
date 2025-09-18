package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class FileDownloadTasklet implements Tasklet {

  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

  @Override
  public RepeatStatus execute(@NotNull StepContribution contribution,
                              @NotNull ChunkContext chunkContext) throws IOException {
    var fileName = chunkContext.getStepContext()
      .getStepExecution()
      .getJobParameters()
      .getString(FILE_URL);

    log.info("Starting file download {}", fileName);

    if (fileName == null) {
      throw new IllegalArgumentException("File name is empty");
    }

    var resource = new ClassPathResource(fileName);
    if (!resource.exists()) {
      throw new IllegalArgumentException("File not found: " + fileName);
    }

    var targetFile = new File(TMP_DIR, fileName);
    Files.copy(resource.getInputStream(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

    return RepeatStatus.FINISHED;
  }

}
