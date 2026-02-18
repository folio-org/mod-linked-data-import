package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_NAME;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.folio.linked.data.imprt.service.s3.S3Service;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;

@UnitTest
@ExtendWith(MockitoExtension.class)
class FileDownloadTaskletTest {

  @InjectMocks
  private FileDownloadTasklet fileDownloadTasklet;
  @Mock
  private S3Service s3Service;

  @Test
  void shouldTriggerS3ServiceDownloadWithCorrectParameters() throws IOException {
    // given
    var fileName = "fileName";
    var jobParameters = mock(JobParameters.class);
    var chunkContext = mock(ChunkContext.class);
    var stepContext = mock(StepContext.class);
    var stepExecution = mock(StepExecution.class);
    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobParameters()).thenReturn(jobParameters);
    when(jobParameters.getString(FILE_NAME)).thenReturn(fileName);
    var stepContribution = mock(StepContribution.class);

    // when
    fileDownloadTasklet.execute(stepContribution, chunkContext);

    // then
    verify(s3Service).download(fileName, TMP_DIR);
  }

  @Test
  void shouldThrowIllegalArgumentException_ifS3ServiceThrowsIoException() throws IOException {
    // given
    var fileName = "fileName";
    var chunkContext = mock(ChunkContext.class);
    var stepContext = mock(StepContext.class);
    var stepExecution = mock(StepExecution.class);
    var jobParameters = mock(JobParameters.class);
    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobParameters()).thenReturn(jobParameters);
    when(jobParameters.getString(FILE_NAME)).thenReturn(fileName);
    doThrow(new IOException()).when(s3Service).download(fileName, TMP_DIR);
    var stepContribution = mock(StepContribution.class);

    // when
    var thrown = assertThrows(IllegalArgumentException.class,
      () -> fileDownloadTasklet.execute(stepContribution, chunkContext));

    // then
    assertThat(thrown.getMessage()).isEqualTo("Error downloading file " + fileName);
  }
}
