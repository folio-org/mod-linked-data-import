package org.folio.linked.data.imprt.service.cleanup;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.folio.linked.data.imprt.model.entity.BatchJobExecution;
import org.folio.linked.data.imprt.repo.BatchJobExecutionRepo;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;

@ExtendWith(MockitoExtension.class)
class DataCleanupServiceTest {

  @Mock
  private BatchJobExecutionRepo batchJobExecutionRepo;

  @Mock
  private RdfFileLineRepo rdfFileLineRepo;

  @InjectMocks
  private DataCleanupService dataCleanupService;

  @ParameterizedTest
  @EnumSource(BatchStatus.class)
  void shouldOnlyCleanupApplicableJobs(BatchStatus status) {
    // Given
    var job = new BatchJobExecution();
    job.setJobExecutionId(1L);
    job.setStatus(status);
    var applicableStatuses = Set.of(
      BatchStatus.ABANDONED,
      BatchStatus.COMPLETED,
      BatchStatus.FAILED,
      BatchStatus.STOPPED
    );

    when(batchJobExecutionRepo.findAll()).thenReturn(List.of(job));

    // When
    dataCleanupService.cleanupCompletedJobData();

    // Then
    if (applicableStatuses.contains(status)) {
      verify(rdfFileLineRepo, times(1)).deleteByJobExecutionId(1L);
    } else {
      verify(rdfFileLineRepo, never()).deleteByJobExecutionId(1L);
    }
  }
}

