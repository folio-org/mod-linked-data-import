package org.folio.linked.data.imprt.service.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.STARTED_BY;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.folio.linked.data.imprt.model.entity.BatchJobExecution;
import org.folio.linked.data.imprt.model.entity.FailedRdfLine;
import org.folio.linked.data.imprt.model.entity.ImportResultEvent;
import org.folio.linked.data.imprt.repo.BatchJobExecutionParamsRepo;
import org.folio.linked.data.imprt.repo.BatchJobExecutionRepo;
import org.folio.linked.data.imprt.repo.BatchStepExecutionRepo;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;

@UnitTest
@ExtendWith(MockitoExtension.class)
class JobServiceImplTest {

  @Mock
  private BatchJobExecutionRepo batchJobExecutionRepo;
  @Mock
  private BatchJobExecutionParamsRepo batchJobExecutionParamsRepo;
  @Mock
  private BatchStepExecutionRepo batchStepExecutionRepo;
  @Mock
  private ImportResultEventRepo importResultEventRepo;
  @Mock
  private FailedRdfLineRepo failedRdfLineRepo;
  @Mock
  private JobOperator jobOperator;
  @Mock
  private JobExplorer jobExplorer;
  @InjectMocks
  private JobServiceImpl jobInfoService;

  @Test
  void getJobInfo_shouldReturnJobInfoWithNullValues_givenNoParameters() {
    // given
    var jobExecutionId = 456L;
    var jobExecution = new BatchJobExecution();
    jobExecution.setJobExecutionId(jobExecutionId);
    jobExecution.setStartTime(LocalDateTime.of(2025, 12, 9, 8, 0, 0));
    jobExecution.setEndTime(LocalDateTime.of(2025, 12, 9, 9, 30, 0));
    jobExecution.setStatus(BatchStatus.FAILED);

    when(batchJobExecutionRepo.findByJobExecutionId(jobExecutionId))
      .thenReturn(Optional.of(jobExecution));
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL))
      .thenReturn(Optional.empty());
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, STARTED_BY))
      .thenReturn(Optional.empty());
    when(batchStepExecutionRepo.getTotalReadCountByJobExecutionId(jobExecutionId))
      .thenReturn(0L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobExecutionId))
      .thenReturn(0L);
    when(importResultEventRepo.findAllByJobExecutionId(jobExecutionId))
      .thenReturn(List.of());

    // when
    var result = jobInfoService.getJobInfo(jobExecutionId);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getStartDate()).isEqualTo("2025-12-09T08:00");
    assertThat(result.getEndDate()).isEqualTo("2025-12-09T09:30");
    assertThat(result.getStartedBy()).isNull();
    assertThat(result.getStatus()).isEqualTo("FAILED");
    assertThat(result.getFileName()).isNull();
    assertThat(result.getCurrentStep()).isNull();
    assertThat(result.getLinesRead()).isZero();
    assertThat(result.getLinesMapped()).isZero();
    assertThat(result.getLinesFailedMapping()).isZero();
    assertThat(result.getLinesCreated()).isZero();
    assertThat(result.getLinesUpdated()).isZero();
    assertThat(result.getLinesFailedSaving()).isZero();
  }

  @Test
  void getJobInfo_shouldThrowException_givenJobNotFound() {
    // given
    var jobExecutionId = 999L;
    when(batchJobExecutionRepo.findByJobExecutionId(jobExecutionId))
      .thenReturn(Optional.empty());

    // when & then
    assertThatThrownBy(() -> jobInfoService.getJobInfo(jobExecutionId))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Job execution not found for jobExecutionId: 999");
  }

  @Test
  void getJobInfo_shouldCalculateCorrectly_givenEmptyFailedRdfLines() {
    // given
    var jobExecutionId = 456L;
    var jobExecution = new BatchJobExecution();
    jobExecution.setJobExecutionId(jobExecutionId);
    jobExecution.setStartTime(LocalDateTime.of(2025, 12, 9, 15, 45, 30));
    jobExecution.setEndTime(LocalDateTime.of(2025, 12, 9, 15, 50, 0));
    jobExecution.setStatus(BatchStatus.FAILED);

    var importResultEvent = new ImportResultEvent()
      .setJobExecutionId(jobExecutionId)
      .setResourcesCount(10)
      .setCreatedCount(10)
      .setUpdatedCount(0)
      .setFailedRdfLines(Set.of());

    when(batchJobExecutionRepo.findByJobExecutionId(jobExecutionId))
      .thenReturn(Optional.of(jobExecution));
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL))
      .thenReturn(Optional.of("file.rdf"));
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, STARTED_BY))
      .thenReturn(Optional.of("user-123"));
    when(batchStepExecutionRepo.getTotalReadCountByJobExecutionId(jobExecutionId))
      .thenReturn(15L);
    when(batchStepExecutionRepo.getTotalWriteCountByJobExecutionId(jobExecutionId))
      .thenReturn(10L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobExecutionId))
      .thenReturn(5L);
    when(importResultEventRepo.findAllByJobExecutionId(jobExecutionId))
      .thenReturn(List.of(importResultEvent));

    // when
    var result = jobInfoService.getJobInfo(jobExecutionId);

    // then
    assertThat(result.getLinesFailedSaving()).isZero();
    assertThat(result.getLinesFailedMapping()).isEqualTo(5L);
    assertThat(result.getStatus()).isEqualTo("FAILED");
  }

  @Test
  void getJobInfo_shouldHandleMultipleImportResults() {
    // given
    var jobExecutionId = 789L;
    var jobExecution = new BatchJobExecution();
    jobExecution.setJobExecutionId(jobExecutionId);
    jobExecution.setStartTime(LocalDateTime.of(2025, 12, 9, 8, 0, 0));
    jobExecution.setStatus(BatchStatus.STARTED);

    var importResults = List.of(
      createImportResultEvent(jobExecutionId, 1000, 800, 200, 10),
      createImportResultEvent(jobExecutionId, 1000, 850, 150, 5),
      createImportResultEvent(jobExecutionId, 500, 400, 100, 3)
    );
    var startedBy = UUID.randomUUID().toString();
    String fileUrl = "large-file.rdf";
    when(batchJobExecutionRepo.findByJobExecutionId(jobExecutionId))
      .thenReturn(Optional.of(jobExecution));
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL))
      .thenReturn(Optional.of(fileUrl));
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, STARTED_BY))
      .thenReturn(Optional.of(startedBy));
    when(batchStepExecutionRepo.getTotalReadCountByJobExecutionId(jobExecutionId))
      .thenReturn(2500L);
    when(batchStepExecutionRepo.getTotalWriteCountByJobExecutionId(jobExecutionId))
      .thenReturn(2482L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobExecutionId))
      .thenReturn(0L);
    when(importResultEventRepo.findAllByJobExecutionId(jobExecutionId))
      .thenReturn(importResults);
    when(batchStepExecutionRepo.findLastStepNameByJobExecutionId(jobExecutionId))
      .thenReturn(Optional.of("cleaningStep"));

    // when
    var result = jobInfoService.getJobInfo(jobExecutionId);

    // then
    assertThat(result.getStartDate()).isEqualTo("2025-12-09T08:00");
    assertThat(result.getStartedBy()).isEqualTo(startedBy);
    assertThat(result.getStatus()).isEqualTo("STARTED");
    assertThat(result.getFileName()).isEqualTo(fileUrl);
    assertThat(result.getCurrentStep()).isEqualTo("cleaningStep");
    assertThat(result.getLinesRead()).isEqualTo(2500L);
    assertThat(result.getLinesFailedMapping()).isZero();
    assertThat(result.getLinesMapped()).isEqualTo(2482L);
    assertThat(result.getLinesCreated()).isEqualTo(2050L); // 800 + 850 + 400
    assertThat(result.getLinesUpdated()).isEqualTo(450L); // 200 + 150 + 100
    assertThat(result.getLinesFailedSaving()).isEqualTo(18L); // 10 + 5 + 3
  }

  @Test
  void generateFailedLinesCsv_shouldReturnCsvWithHeader_givenNoFailedLines() throws Exception {
    // given
    var jobExecutionId = 123L;
    when(failedRdfLineRepo.findAllByJobExecutionIdOrderByLineNumber(jobExecutionId)).thenReturn(Stream.empty());

    // when
    var result = jobInfoService.generateFailedLinesCsv(jobExecutionId);

    // then
    var content = new String(result.getInputStream().readAllBytes());
    assertThat(content).isEqualTo("lineNumber,description,failedRdfLine\n");
  }

  @Test
  void generateFailedLinesCsv_shouldReturnCsvWithData_givenFailedLines() throws Exception {
    // given
    var jobExecutionId = 123L;
    var line1 = new FailedRdfLine()
      .setLineNumber(5L)
      .setDescription("RDF parsing error")
      .setFailedRdfLine("{\"@id\": \"invalid\"}");

    var line2 = new FailedRdfLine()
      .setLineNumber(10L)
      .setDescription("Mapping failed")
      .setFailedRdfLine("{\"test\": \"data\"}");

    when(failedRdfLineRepo.findAllByJobExecutionIdOrderByLineNumber(jobExecutionId))
      .thenReturn(Stream.of(line1, line2));

    // when
    var result = jobInfoService.generateFailedLinesCsv(jobExecutionId);

    // then
    var content = new String(result.getInputStream().readAllBytes());
    assertThat(content).isEqualTo("""
      lineNumber,description,failedRdfLine
      5,RDF parsing error,"{""@id"": ""invalid""}"
      10,Mapping failed,"{""test"": ""data""}"
      """);
  }

  @Test
  void generateFailedLinesCsv_shouldEscapeSpecialCharacters_givenCsvSpecialChars() throws Exception {
    // given
    var jobExecutionId = 123L;
    var line = new FailedRdfLine()
      .setLineNumber(1L)
      .setDescription("Error with, comma and \"quotes\"")
      .setFailedRdfLine("Line with\nnewline");

    when(failedRdfLineRepo.findAllByJobExecutionIdOrderByLineNumber(jobExecutionId))
      .thenReturn(Stream.of(line));

    // when
    var result = jobInfoService.generateFailedLinesCsv(jobExecutionId);

    // then
    var content = new String(result.getInputStream().readAllBytes());
    assertThat(content).contains("\"Error with, comma and \"\"quotes\"\"\"")
      .contains("\"Line with\nnewline\"");
  }

  @Test
  void generateFailedLinesCsv_shouldHandleNullValues_givenNullFields() throws Exception {
    // given
    var jobExecutionId = 123L;
    var line = new FailedRdfLine()
      .setLineNumber(1L)
      .setDescription(null)
      .setFailedRdfLine(null);

    when(failedRdfLineRepo.findAllByJobExecutionIdOrderByLineNumber(jobExecutionId))
      .thenReturn(Stream.of(line));

    // when
    var result = jobInfoService.generateFailedLinesCsv(jobExecutionId);

    // then
    var content = new String(result.getInputStream().readAllBytes());
    assertThat(content).isEqualTo("lineNumber,description,failedRdfLine\n1,,\n");
  }

  private ImportResultEvent createImportResultEvent(Long jobExecutionId, int resources, int created, int updated,
                                                    int failed) {
    var failedLines = IntStream.range(0, failed)
      .mapToObj(i -> new FailedRdfLine().setLineNumber(i + 1L))
      .collect(Collectors.toCollection(LinkedHashSet::new));
    return new ImportResultEvent()
      .setJobExecutionId(jobExecutionId)
      .setResourcesCount(resources)
      .setCreatedCount(created)
      .setUpdatedCount(updated)
      .setFailedRdfLines(failedLines);
  }

  @Test
  void cancelJob_shouldStopJob_givenRunningJob() throws Exception {
    // given
    var jobExecutionId = 123L;
    var jobExecution = new JobExecution(jobExecutionId);
    jobExecution.setStatus(BatchStatus.STARTED);

    when(jobExplorer.getJobExecution(jobExecutionId)).thenReturn(jobExecution);

    // when
    jobInfoService.cancelJob(jobExecutionId);

    // then
    org.mockito.Mockito.verify(jobOperator).stop(jobExecutionId);
  }

  @Test
  void cancelJob_shouldThrowException_givenJobNotFound() {
    // given
    var jobExecutionId = 999L;
    when(jobExplorer.getJobExecution(jobExecutionId)).thenReturn(null);

    // when & then
    assertThatThrownBy(() -> jobInfoService.cancelJob(jobExecutionId))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Job execution not found for jobExecutionId: 999");
  }

  @Test
  void cancelJob_shouldThrowException_givenCompletedJob() {
    // given
    var jobExecutionId = 123L;
    var jobExecution = new JobExecution(jobExecutionId);
    jobExecution.setStatus(BatchStatus.COMPLETED);

    when(jobExplorer.getJobExecution(jobExecutionId)).thenReturn(jobExecution);

    // when & then
    assertThatThrownBy(() -> jobInfoService.cancelJob(jobExecutionId))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("Job execution 123 is not running. Current status: COMPLETED");
  }

  @Test
  void cancelJob_shouldThrowException_givenFailedJob() {
    // given
    var jobExecutionId = 456L;
    var jobExecution = new JobExecution(jobExecutionId);
    jobExecution.setStatus(BatchStatus.FAILED);

    when(jobExplorer.getJobExecution(jobExecutionId)).thenReturn(jobExecution);

    // when & then
    assertThatThrownBy(() -> jobInfoService.cancelJob(jobExecutionId))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("Job execution 456 is not running. Current status: FAILED");
  }

  @Test
  void cancelJob_shouldThrowException_givenJobOperatorThrowsNoSuchJobExecutionException() throws Exception {
    // given
    var jobExecutionId = 123L;
    var jobExecution = new JobExecution(jobExecutionId);
    jobExecution.setStatus(BatchStatus.STARTED);

    when(jobExplorer.getJobExecution(jobExecutionId)).thenReturn(jobExecution);
    when(jobOperator.stop(jobExecutionId))
      .thenThrow(new NoSuchJobExecutionException("Job not found"));

    // when & then
    assertThatThrownBy(() -> jobInfoService.cancelJob(jobExecutionId))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Job execution not found for jobExecutionId: 123")
      .hasCauseInstanceOf(NoSuchJobExecutionException.class);
  }
}


