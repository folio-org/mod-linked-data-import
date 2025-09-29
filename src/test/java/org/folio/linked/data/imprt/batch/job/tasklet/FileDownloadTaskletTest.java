package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
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
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;

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
    var fileUrl = "s3://bucket/key";
    var jobParameters = mock(JobParameters.class);
    var chunkContext = mock(ChunkContext.class);
    var stepContext = mock(StepContext.class);
    var stepExecution = mock(StepExecution.class);
    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobParameters()).thenReturn(jobParameters);
    when(jobParameters.getString(FILE_URL)).thenReturn(fileUrl);
    var stepContribution = mock(StepContribution.class);

    // when
    fileDownloadTasklet.execute(stepContribution, chunkContext);

    // then
    verify(s3Service).download(fileUrl, TMP_DIR);
  }

  @Test
  void shouldThrowIllegalArgumentException_ifS3ServiceThrowsIoException() throws IOException {
    // given
    var fileUrl = "s3://bucket/key";
    var chunkContext = mock(ChunkContext.class);
    var stepContext = mock(StepContext.class);
    var stepExecution = mock(StepExecution.class);
    var jobParameters = mock(JobParameters.class);
    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobParameters()).thenReturn(jobParameters);
    when(jobParameters.getString(FILE_URL)).thenReturn(fileUrl);
    doThrow(new IOException()).when(s3Service).download(fileUrl, TMP_DIR);
    var stepContribution = mock(StepContribution.class);

    // when
    var thrown = assertThrows(IllegalArgumentException.class,
      () -> fileDownloadTasklet.execute(stepContribution, chunkContext));

    // then
    assertThat(thrown.getMessage()).isEqualTo("Error downloading file " + fileUrl);
  }
}
