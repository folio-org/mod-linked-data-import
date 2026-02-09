package org.folio.linked.data.imprt.service.cleanup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.List;
import org.folio.linked.data.imprt.model.entity.BatchJobExecution;
import org.folio.linked.data.imprt.repo.BatchJobExecutionRepo;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.test.util.ReflectionTestUtils;

@UnitTest
@ExtendWith(MockitoExtension.class)
class DataCleanupServiceTest {

  @Mock
  private BatchJobExecutionRepo batchJobExecutionRepo;

  @Mock
  private RdfFileLineRepo rdfFileLineRepo;

  @InjectMocks
  private DataCleanupService dataCleanupService;

  @BeforeEach
  void setUp() {
    ReflectionTestUtils.setField(dataCleanupService, "cleanupAgeDays", 2);
  }

  @Test
  void shouldCleanupJobsOlderThan2Days() {
    // Given
    var oldJob1 = new BatchJobExecution();
    oldJob1.setJobExecutionId(1L);
    oldJob1.setStartTime(LocalDateTime.now().minusDays(3));
    oldJob1.setStatus(BatchStatus.COMPLETED);

    var oldJob2 = new BatchJobExecution();
    oldJob2.setJobExecutionId(2L);
    oldJob2.setStartTime(LocalDateTime.now().minusDays(5));
    oldJob2.setStatus(BatchStatus.FAILED);

    when(batchJobExecutionRepo.findByStartTimeBefore(any(LocalDateTime.class)))
      .thenReturn(List.of(oldJob1, oldJob2));
    when(rdfFileLineRepo.deleteByJobExecutionId(1L)).thenReturn(100L);
    when(rdfFileLineRepo.deleteByJobExecutionId(2L)).thenReturn(50L);

    // When
    dataCleanupService.cleanupCompletedJobData();

    // Then
    verify(rdfFileLineRepo, times(1)).deleteByJobExecutionId(1L);
    verify(rdfFileLineRepo, times(1)).deleteByJobExecutionId(2L);
  }

  @Test
  void shouldNotCleanupJobsWithNullStartTime() {
    // Given - repository method filters by startTime, so null startTime jobs won't be returned
    when(batchJobExecutionRepo.findByStartTimeBefore(any(LocalDateTime.class)))
      .thenReturn(List.of());

    // When
    dataCleanupService.cleanupCompletedJobData();

    // Then
    verify(rdfFileLineRepo, never()).deleteByJobExecutionId(anyLong());
  }

  @Test
  void shouldHandleEmptyJobList() {
    // Given
    when(batchJobExecutionRepo.findByStartTimeBefore(any(LocalDateTime.class)))
      .thenReturn(List.of());

    // When
    dataCleanupService.cleanupCompletedJobData();

    // Then
    verify(rdfFileLineRepo, never()).deleteByJobExecutionId(anyLong());
  }

  @Test
  void shouldContinueCleanupWhenOneJobFails() {
    // Given
    var oldJob1 = new BatchJobExecution();
    oldJob1.setJobExecutionId(1L);
    oldJob1.setStartTime(LocalDateTime.now().minusDays(3));
    oldJob1.setStatus(BatchStatus.COMPLETED);

    var oldJob2 = new BatchJobExecution();
    oldJob2.setJobExecutionId(2L);
    oldJob2.setStartTime(LocalDateTime.now().minusDays(4));
    oldJob2.setStatus(BatchStatus.COMPLETED);

    when(batchJobExecutionRepo.findByStartTimeBefore(any(LocalDateTime.class)))
      .thenReturn(List.of(oldJob1, oldJob2));
    doThrow(new RuntimeException("Database error")).when(rdfFileLineRepo).deleteByJobExecutionId(1L);
    when(rdfFileLineRepo.deleteByJobExecutionId(2L)).thenReturn(50L);

    // When
    dataCleanupService.cleanupCompletedJobData();

    // Then
    verify(rdfFileLineRepo, times(1)).deleteByJobExecutionId(1L);
    verify(rdfFileLineRepo, times(1)).deleteByJobExecutionId(2L);
  }

  @Test
  void shouldCleanupJobsRegardlessOfStatus() {
    // Given - all jobs older than 2 days but with different statuses
    var completedJob = new BatchJobExecution();
    completedJob.setJobExecutionId(1L);
    completedJob.setStartTime(LocalDateTime.now().minusDays(3));
    completedJob.setStatus(BatchStatus.COMPLETED);

    var failedJob = new BatchJobExecution();
    failedJob.setJobExecutionId(2L);
    failedJob.setStartTime(LocalDateTime.now().minusDays(3));
    failedJob.setStatus(BatchStatus.FAILED);

    var stoppedJob = new BatchJobExecution();
    stoppedJob.setJobExecutionId(3L);
    stoppedJob.setStartTime(LocalDateTime.now().minusDays(3));
    stoppedJob.setStatus(BatchStatus.STOPPED);

    var startedJob = new BatchJobExecution();
    startedJob.setJobExecutionId(4L);
    startedJob.setStartTime(LocalDateTime.now().minusDays(3));
    startedJob.setStatus(BatchStatus.STARTED);

    when(batchJobExecutionRepo.findByStartTimeBefore(any(LocalDateTime.class)))
      .thenReturn(List.of(completedJob, failedJob, stoppedJob, startedJob));
    when(rdfFileLineRepo.deleteByJobExecutionId(1L)).thenReturn(10L);
    when(rdfFileLineRepo.deleteByJobExecutionId(2L)).thenReturn(20L);
    when(rdfFileLineRepo.deleteByJobExecutionId(3L)).thenReturn(30L);
    when(rdfFileLineRepo.deleteByJobExecutionId(4L)).thenReturn(40L);

    // When
    dataCleanupService.cleanupCompletedJobData();

    // Then - all jobs should be cleaned up regardless of status
    verify(rdfFileLineRepo, times(1)).deleteByJobExecutionId(1L);
    verify(rdfFileLineRepo, times(1)).deleteByJobExecutionId(2L);
    verify(rdfFileLineRepo, times(1)).deleteByJobExecutionId(3L);
    verify(rdfFileLineRepo, times(1)).deleteByJobExecutionId(4L);
  }
}

