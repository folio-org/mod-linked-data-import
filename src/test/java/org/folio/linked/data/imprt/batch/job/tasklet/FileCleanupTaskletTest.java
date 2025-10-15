package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;

@UnitTest
class FileCleanupTaskletTest {

  private final FileCleanupTasklet tasklet = new FileCleanupTasklet();

  @Test
  void shouldDeleteExistingTempFile_givenFileExists() throws IOException {
    // given
    var fileName = "temp_test_file.txt";
    var tempFile = new File(TMP_DIR, fileName);
    Files.writeString(tempFile.toPath(), "data");
    assertThat(tempFile.exists()).isTrue();
    var fileUrl = "s3://bucket/" + fileName;
    var chunkContext = mockChunkContext(fileUrl);
    var stepContribution = mock(StepContribution.class);

    // when
    tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(tempFile.exists()).isFalse();
  }

  private ChunkContext mockChunkContext(String fileUrl) {
    var chunkContext = mock(ChunkContext.class);
    var stepContext = mock(StepContext.class);
    var stepExecution = mock(StepExecution.class);
    var jobParameters = mock(JobParameters.class);
    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobParameters()).thenReturn(jobParameters);
    when(jobParameters.getString(FILE_URL)).thenReturn(fileUrl);
    return chunkContext;
  }
}
