package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_NAME;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.model.entity.RdfFileLine;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Log4j2
@Component
@RequiredArgsConstructor
public class FileToDatabaseTasklet implements Tasklet {
  private static final int BATCH_SIZE = 1000;
  private final RdfFileLineRepo rdfFileLineRepo;

  @Override
  @Transactional
  public RepeatStatus execute(@NotNull StepContribution contribution, @NotNull ChunkContext chunkContext) {
    var jobExecutionId = chunkContext.getStepContext().getStepExecution().getJobExecutionId();
    var fileName = chunkContext.getStepContext()
      .getStepExecution()
      .getJobParameters()
      .getString(FILE_NAME);

    if (fileName == null) {
      throw new IllegalArgumentException("fileName parameter is required");
    }

    var file = new File(TMP_DIR, fileName);

    try {
      saveFileToDatabase(file, jobExecutionId);
      deleteFile(file);
    } catch (IOException e) {
      log.error("Error saving file to database: {}", fileName, e);
      throw new IllegalArgumentException("Error saving file to database: " + fileName, e);
    }

    return RepeatStatus.FINISHED;
  }

  private void saveFileToDatabase(File file, Long jobExecutionId) throws IOException {
    var batch = new ArrayList<RdfFileLine>(BATCH_SIZE);
    var lineNumber = 1L;

    try (var reader = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        var rdfLine = new RdfFileLine()
          .setJobExecutionId(jobExecutionId)
          .setLineNumber(lineNumber++)
          .setContent(line);
        batch.add(rdfLine);

        if (batch.size() >= BATCH_SIZE) {
          rdfFileLineRepo.saveAll(batch);
          batch.clear();
        }
      }

      if (!batch.isEmpty()) {
        rdfFileLineRepo.saveAll(batch);
      }
    }

    log.info("Saved {} lines to database for job execution {}", lineNumber - 1, jobExecutionId);
  }

  private void deleteFile(File file) {
    try {
      Files.delete(file.toPath());
      log.info("Deleted temporary file: {}", file.getAbsolutePath());
    } catch (IOException e) {
      log.warn("Failed to delete temporary file: {}", file.getAbsolutePath(), e);
    }
  }
}

