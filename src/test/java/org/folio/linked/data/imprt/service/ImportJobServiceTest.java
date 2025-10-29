package org.folio.linked.data.imprt.service;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.CONTENT_TYPE;
import static org.folio.linked.data.imprt.batch.job.Parameters.DATE_START;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.stream.Stream;
import org.folio.linked.data.imprt.service.imprt.ImportJobServiceImpl;
import org.folio.linked.data.imprt.service.s3.S3Service;
import org.folio.spring.exception.NotFoundException;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;

@UnitTest
@ExtendWith(MockitoExtension.class)
class ImportJobServiceTest {

  @InjectMocks
  private ImportJobServiceImpl importJobService;
  @Mock
  private Job rdfImportJob;
  @Mock
  private JobLauncher jobLauncher;
  @Mock
  private S3Service s3Service;

  public static Stream<Arguments> jobLaunchExceptions() {
    return Stream.of(
      arguments(new JobExecutionAlreadyRunningException("1")),
      arguments(new JobRestartException("2")),
      arguments(new JobInstanceAlreadyCompleteException("3")),
      arguments(new JobParametersInvalidException("4"))
    );
  }

  @Test
  void start_shouldInvokeJobLauncherRun_ifGivenFileExists() throws JobInstanceAlreadyCompleteException,
    JobExecutionAlreadyRunningException, JobParametersInvalidException, JobRestartException {
    // given
    var fileUrl = "http://example.com/file";
    doReturn(true).when(s3Service).exists(fileUrl);
    var jobExecutionMock = mock(JobExecution.class);
    doReturn(jobExecutionMock).when(jobLauncher).run(any(), any());
    var jobInstanceMock = mock(JobInstance.class);
    doReturn(jobInstanceMock).when(jobExecutionMock).getJobInstance();
    doReturn(123L).when(jobInstanceMock).getInstanceId();
    var contentType = "application/json";

    // when
    importJobService.start(fileUrl, contentType);

    // then
    var jobParametersCaptor = ArgumentCaptor.forClass(JobParameters.class);
    verify(jobLauncher).run(eq(rdfImportJob), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().getParameter(FILE_URL).getValue()).isEqualTo(fileUrl);
    assertThat(jobParametersCaptor.getValue().getParameter(CONTENT_TYPE).getValue()).isEqualTo(contentType);
    assertThat(jobParametersCaptor.getValue().getParameter(DATE_START)).isNotNull();
  }

  @Test
  void start_shouldInvokeJobLauncherRunWithDefaultContentType_ifGivenFileExistsAndNoContentTypeProvided()
    throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException, JobParametersInvalidException,
    JobRestartException {
    // given
    var fileUrl = "http://example.com/file";
    doReturn(true).when(s3Service).exists(fileUrl);
    var jobExecutionMock = mock(JobExecution.class);
    doReturn(jobExecutionMock).when(jobLauncher).run(any(), any());
    var jobInstanceMock = mock(JobInstance.class);
    doReturn(jobInstanceMock).when(jobExecutionMock).getJobInstance();
    doReturn(123L).when(jobInstanceMock).getInstanceId();

    // when
    importJobService.start(fileUrl, null);

    // then
    var jobParametersCaptor = ArgumentCaptor.forClass(JobParameters.class);
    verify(jobLauncher).run(eq(rdfImportJob), jobParametersCaptor.capture());
    assertThat(jobParametersCaptor.getValue().getParameter(FILE_URL).getValue()).isEqualTo(fileUrl);
    assertThat(jobParametersCaptor.getValue().getParameter(CONTENT_TYPE).getValue()).isEqualTo("application/ld+json");
    assertThat(jobParametersCaptor.getValue().getParameter(DATE_START)).isNotNull();
  }

  @Test
  void start_shouldThrowIllegalArgumentException_ifGivenFileUrlIsEmpty() {
    // given
    var fileUrl = "";

    // when
    var thrown = assertThrows(IllegalArgumentException.class,
      () -> importJobService.start(fileUrl, null));

    // then
    assertThat(thrown.getMessage()).isEqualTo("File URL should be provided");
  }

  @Test
  void start_shouldThrowNotFoundException_ifGivenFileDoesNotExist() {
    // given
    var fileUrl = "http://example.com/file";
    doReturn(false).when(s3Service).exists(fileUrl);

    // when
    var thrown = assertThrows(NotFoundException.class,
      () -> importJobService.start(fileUrl, null));

    // then
    assertThat(thrown.getMessage()).isEqualTo("File with provided URL doesn't exist: " + fileUrl);
  }

  @ParameterizedTest
  @MethodSource("jobLaunchExceptions")
  void start_shouldThrowIllegalArgumentException_ifJobLauncherThrowsException(Exception e)
    throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException,
    JobParametersInvalidException, JobRestartException {
    // given
    var fileUrl = "http://example.com/file";
    doReturn(true).when(s3Service).exists(fileUrl);
    doThrow(e).when(jobLauncher).run(any(), any());

    // when
    var thrown = assertThrows(IllegalArgumentException.class,
      () -> importJobService.start(fileUrl, null));

    // then
    assertThat(thrown.getMessage()).isEqualTo("Job launch exception");
    assertThat(thrown.getCause()).isEqualTo(e);
  }
}
