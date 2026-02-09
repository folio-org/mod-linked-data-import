package org.folio.linked.data.imprt.service.imprt;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.CONTENT_TYPE;
import static org.folio.linked.data.imprt.batch.job.Parameters.DEFAULT_WORK_TYPE;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_NAME;
import static org.folio.linked.data.imprt.batch.job.Parameters.STARTED_BY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.util.Properties;
import java.util.stream.Stream;
import org.folio.linked.data.imprt.domain.dto.DefaultWorkType;
import org.folio.linked.data.imprt.service.s3.S3Service;
import org.folio.spring.FolioExecutionContext;
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
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;

@UnitTest
@ExtendWith(MockitoExtension.class)
class ImportJobServiceTest {

  @InjectMocks
  private ImportJobServiceImpl importJobService;
  @Mock
  private Job rdfImportJob;
  @Mock
  private JobOperator jobOperator;
  @Mock
  private S3Service s3Service;
  @Mock
  private FolioExecutionContext folioExecutionContext;

  public static Stream<Arguments> jobLaunchExceptions() {
    return Stream.of(
      arguments(new NoSuchJobException("Job not found")),
      arguments(new JobInstanceAlreadyExistsException("Job instance already exists")),
      arguments(new JobParametersInvalidException("Invalid parameters"))
    );
  }

  @Test
  void start_shouldInvokeJobOperatorStart_ifGivenFileExists() throws Exception {
    // given
    var fileName = "http://example.com/file";
    doReturn(true).when(s3Service).exists(fileName);
    var userId = java.util.UUID.randomUUID();
    doReturn(userId).when(folioExecutionContext).getUserId();
    var jobName = "testJob";
    doReturn(jobName).when(rdfImportJob).getName();
    doReturn(123L).when(jobOperator).start(eq(jobName), any(Properties.class));
    var contentType = "application/json";
    var defaultWorkType = DefaultWorkType.MONOGRAPH;

    // when
    var result = importJobService.start(fileName, contentType, defaultWorkType);

    // then
    assertThat(result).isEqualTo(123L);
    var propertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(jobOperator).start(eq(jobName), propertiesCaptor.capture());
    var capturedProps = propertiesCaptor.getValue();
    assertThat(capturedProps.getProperty(FILE_NAME)).isEqualTo(fileName);
    assertThat(capturedProps.getProperty(CONTENT_TYPE)).isEqualTo(contentType);
    assertThat(capturedProps.getProperty(STARTED_BY)).isEqualTo(userId.toString());
    assertThat(capturedProps.getProperty(DEFAULT_WORK_TYPE)).isEqualTo(defaultWorkType.name());
    assertThat(capturedProps.getProperty("run.timestamp")).isNotNull();
  }

  @Test
  void start_shouldInvokeJobOperatorStartWithDefaultContentType_ifGivenFileExistsAndNoContentTypeProvided()
    throws Exception {
    // given
    var fileName = "http://example.com/file";
    doReturn(true).when(s3Service).exists(fileName);
    var userId = java.util.UUID.randomUUID();
    doReturn(userId).when(folioExecutionContext).getUserId();
    var jobName = "testJob";
    doReturn(jobName).when(rdfImportJob).getName();
    doReturn(123L).when(jobOperator).start(eq(jobName), any(Properties.class));
    var defaultWorkType = DefaultWorkType.MONOGRAPH;

    // when
    var result = importJobService.start(fileName, null, defaultWorkType);

    // then
    assertThat(result).isEqualTo(123L);
    var propertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(jobOperator).start(eq(jobName), propertiesCaptor.capture());
    var capturedProps = propertiesCaptor.getValue();
    assertThat(capturedProps.getProperty(FILE_NAME)).isEqualTo(fileName);
    assertThat(capturedProps.getProperty(CONTENT_TYPE)).isEqualTo("application/ld+json");
    assertThat(capturedProps.getProperty(STARTED_BY)).isEqualTo(userId.toString());
    assertThat(capturedProps.getProperty(DEFAULT_WORK_TYPE)).isEqualTo(defaultWorkType.name());
    assertThat(capturedProps.getProperty("run.timestamp")).isNotNull();
  }

  @Test
  void start_shouldThrowIllegalArgumentException_ifGivenfileNameIsEmpty() {
    // given
    var fileName = "";

    // when
    var thrown = assertThrows(IllegalArgumentException.class,
      () -> importJobService.start(fileName, null, null));

    // then
    assertThat(thrown.getMessage()).isEqualTo("File URL should be provided");
  }

  @Test
  void start_shouldThrowNotFoundException_ifGivenFileDoesNotExist() {
    // given
    var fileName = "http://example.com/file";
    doReturn(false).when(s3Service).exists(fileName);

    // when
    var thrown = assertThrows(NotFoundException.class,
      () -> importJobService.start(fileName, null, null));

    // then
    assertThat(thrown.getMessage()).isEqualTo("File with provided URL doesn't exist: " + fileName);
  }

  @ParameterizedTest
  @MethodSource("jobLaunchExceptions")
  void start_shouldThrowIllegalArgumentException_ifJobOperatorThrowsException(Exception e) throws Exception {
    // given
    var fileName = "http://example.com/file";
    doReturn(true).when(s3Service).exists(fileName);
    var jobName = "testJob";
    doReturn(jobName).when(rdfImportJob).getName();
    doThrow(e).when(jobOperator).start(eq(jobName), any(Properties.class));

    // when
    var thrown = assertThrows(IllegalArgumentException.class,
      () -> importJobService.start(fileName, null, null));

    // then
    assertThat(thrown.getMessage()).isEqualTo("Job launch exception");
    assertThat(thrown.getCause()).isEqualTo(e);
  }
}
