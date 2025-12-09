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

@UnitTest
@ExtendWith(MockitoExtension.class)
class JobInfoServiceImplTest {

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
  @InjectMocks
  private JobInfoServiceImpl jobInfoService;

  @Test
  void getJobInfo_shouldReturnJobInfoWithNullValues_givenNoParameters() {
    // given
    var jobId = 123L;
    var jobExecution = new BatchJobExecution();
    jobExecution.setJobInstanceId(jobId);
    jobExecution.setStartTime(LocalDateTime.of(2025, 12, 9, 8, 0, 0));
    jobExecution.setEndTime(LocalDateTime.of(2025, 12, 9, 9, 30, 0));
    jobExecution.setStatus(BatchStatus.FAILED);

    when(batchJobExecutionRepo.findFirstByJobInstanceIdOrderByJobExecutionIdDesc(jobId))
      .thenReturn(Optional.of(jobExecution));
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobId, FILE_URL))
      .thenReturn(Optional.empty());
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobId, STARTED_BY))
      .thenReturn(Optional.empty());
    when(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobId))
      .thenReturn(0L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobId))
      .thenReturn(0L);
    when(importResultEventRepo.findAllByJobInstanceId(jobId))
      .thenReturn(List.of());

    // when
    var result = jobInfoService.getJobInfo(jobId);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getStartDate()).isEqualTo("2025-12-09T08:00");
    assertThat(result.getEndDate()).isEqualTo("2025-12-09T09:30");
    assertThat(result.getStartedBy()).isNull();
    assertThat(result.getStatus()).isEqualTo("FAILED");
    assertThat(result.getFileName()).isNull();
    assertThat(result.getCurrentStep()).isNull();
    assertThat(result.getLinesRead()).isEqualTo(0L);
    assertThat(result.getLinesMapped()).isEqualTo(0L);
    assertThat(result.getLinesFailedMapping()).isEqualTo(0L);
    assertThat(result.getLinesCreated()).isEqualTo(0L);
    assertThat(result.getLinesUpdated()).isEqualTo(0L);
    assertThat(result.getLinesFailedSaving()).isEqualTo(0L);
  }

  @Test
  void getJobInfo_shouldThrowException_givenJobNotFound() {
    // given
    var jobId = 999L;
    when(batchJobExecutionRepo.findFirstByJobInstanceIdOrderByJobExecutionIdDesc(jobId))
      .thenReturn(Optional.empty());

    // when & then
    assertThatThrownBy(() -> jobInfoService.getJobInfo(jobId))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("Job execution not found for jobId: 999");
  }

  @Test
  void getJobInfo_shouldCalculateCorrectly_givenEmptyFailedRdfLines() {
    // given
    var jobId = 123L;
    var jobExecution = new BatchJobExecution();
    jobExecution.setJobInstanceId(jobId);
    jobExecution.setStartTime(LocalDateTime.of(2025, 12, 9, 15, 45, 30));
    jobExecution.setEndTime(LocalDateTime.of(2025, 12, 9, 15, 50, 0));
    jobExecution.setStatus(BatchStatus.FAILED);

    var importResultEvent = new ImportResultEvent()
      .setJobInstanceId(jobId)
      .setResourcesCount(10)
      .setCreatedCount(10)
      .setUpdatedCount(0)
      .setFailedRdfLines(Set.of());

    when(batchJobExecutionRepo.findFirstByJobInstanceIdOrderByJobExecutionIdDesc(jobId))
      .thenReturn(Optional.of(jobExecution));
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobId, FILE_URL))
      .thenReturn(Optional.of("file.rdf"));
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobId, STARTED_BY))
      .thenReturn(Optional.of("user-123"));
    when(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobId))
      .thenReturn(15L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobId))
      .thenReturn(5L);
    when(importResultEventRepo.findAllByJobInstanceId(jobId))
      .thenReturn(List.of(importResultEvent));

    // when
    var result = jobInfoService.getJobInfo(jobId);

    // then
    assertThat(result.getLinesFailedSaving()).isEqualTo(0L);
    assertThat(result.getLinesFailedMapping()).isEqualTo(5L);
    assertThat(result.getStatus()).isEqualTo("FAILED");
  }

  @Test
  void getJobInfo_shouldHandleMultipleImportResults() {
    // given
    var jobId = 456L;
    var jobExecution = new BatchJobExecution();
    jobExecution.setJobInstanceId(jobId);
    jobExecution.setStartTime(LocalDateTime.of(2025, 12, 9, 8, 0, 0));
    jobExecution.setStatus(BatchStatus.STARTED);

    var importResults = List.of(
      createImportResultEvent(jobId, 1000, 800, 200, 10),
      createImportResultEvent(jobId, 1000, 850, 150, 5),
      createImportResultEvent(jobId, 500, 400, 100, 3)
    );
    var startedBy = UUID.randomUUID().toString();
    String fileUrl = "large-file.rdf";
    when(batchJobExecutionRepo.findFirstByJobInstanceIdOrderByJobExecutionIdDesc(jobId))
      .thenReturn(Optional.of(jobExecution));
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobId, FILE_URL))
      .thenReturn(Optional.of(fileUrl));
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobId, STARTED_BY))
      .thenReturn(Optional.of(startedBy));
    when(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobId))
      .thenReturn(2500L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobId))
      .thenReturn(0L);
    when(importResultEventRepo.findAllByJobInstanceId(jobId))
      .thenReturn(importResults);
    when(batchStepExecutionRepo.findLastStepNameByJobInstanceId(jobId))
      .thenReturn(Optional.of("cleaningStep"));

    // when
    var result = jobInfoService.getJobInfo(jobId);

    // then
    assertThat(result.getStartDate()).isEqualTo("2025-12-09T08:00");
    assertThat(result.getStartedBy()).isEqualTo(startedBy);
    assertThat(result.getStatus()).isEqualTo("STARTED");
    assertThat(result.getFileName()).isEqualTo(fileUrl);
    assertThat(result.getCurrentStep()).isEqualTo("cleaningStep");
    assertThat(result.getLinesRead()).isEqualTo(2500L);
    assertThat(result.getLinesFailedMapping()).isEqualTo(0L);
    assertThat(result.getLinesMapped()).isEqualTo(2500L); // 1000 + 1000 + 500
    assertThat(result.getLinesCreated()).isEqualTo(2050L); // 800 + 850 + 400
    assertThat(result.getLinesUpdated()).isEqualTo(450L); // 200 + 150 + 100
    assertThat(result.getLinesFailedSaving()).isEqualTo(18L); // 10 + 5 + 3
  }

  private ImportResultEvent createImportResultEvent(Long jobId, int resources, int created, int updated, int failed) {
    var failedLines = IntStream.range(0, failed)
      .mapToObj(i -> new FailedRdfLine().setLineNumber(i + 1L))
      .collect(Collectors.toCollection(LinkedHashSet::new));
    return new ImportResultEvent()
      .setJobInstanceId(jobId)
      .setResourcesCount(resources)
      .setCreatedCount(created)
      .setUpdatedCount(updated)
      .setFailedRdfLines(failedLines);
  }
}

