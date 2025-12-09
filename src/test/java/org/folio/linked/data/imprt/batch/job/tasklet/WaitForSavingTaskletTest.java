package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.folio.linked.data.imprt.repo.BatchStepExecutionRepo;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.test.util.ReflectionTestUtils;

@UnitTest
@ExtendWith(MockitoExtension.class)
class WaitForSavingTaskletTest {

  @Mock
  private BatchStepExecutionRepo batchStepExecutionRepo;
  @Mock
  private ImportResultEventRepo importResultEventRepo;
  @Mock
  private FailedRdfLineRepo failedRdfLineRepo;
  @InjectMocks
  private WaitForSavingTasklet tasklet;

  @BeforeEach
  void setUp() {
    ReflectionTestUtils.setField(tasklet, "waitIntervalMs", 100);
  }

  @ParameterizedTest
  @CsvSource({
    "100, 90, 10, FINISHED",    // processing complete: 90 + 10 = 100
    "100, 40, 5, CONTINUABLE",   // processing incomplete: 40 + 5 < 100
    "100, 150, 10, FINISHED",    // processed count exceeds read count
    "0, 0, 0, FINISHED",         // no lines read
    "100, 0, 100, FINISHED"      // only failed lines
  })
  void execute_shouldReturnCorrectStatus_givenDifferentCounts(
    long readCount, long processedCount, long failedCount, RepeatStatus expectedStatus) throws InterruptedException {
    // given
    var jobInstanceId = 123L;
    var chunkContext = mockChunkContext(jobInstanceId);
    var stepContribution = mock(StepContribution.class);
    when(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobInstanceId)).thenReturn(readCount);
    if (readCount > 0) {
      when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(processedCount);
      when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(failedCount);
    }

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(expectedStatus);
  }


  private ChunkContext mockChunkContext(Long jobInstanceId) {
    var chunkContext = mock(ChunkContext.class);
    var stepContext = mock(StepContext.class);
    var stepExecution = mock(StepExecution.class);
    var jobExecution = mock(JobExecution.class);
    var jobInstance = mock(JobInstance.class);
    var executionContext = new ExecutionContext();

    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobExecution()).thenReturn(jobExecution);
    lenient().when(stepExecution.getExecutionContext()).thenReturn(executionContext);
    when(jobExecution.getJobInstance()).thenReturn(jobInstance);
    when(jobInstance.getInstanceId()).thenReturn(jobInstanceId);
    return chunkContext;
  }
}

