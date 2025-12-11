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
import org.junit.jupiter.api.Test;
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

  @Test
  void execute_shouldReturnFinished_givenNoLinesRead() throws InterruptedException {
    // given
    var jobInstanceId = 123L;
    var chunkContext = mockChunkContext(jobInstanceId);
    var stepContribution = mock(StepContribution.class);
    when(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobInstanceId)).thenReturn(0L);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.FINISHED);
  }

  @Test
  void execute_shouldReturnFinished_givenAllLinesProcessed() throws InterruptedException {
    // given
    var jobInstanceId = 123L;
    var chunkContext = mockChunkContext(jobInstanceId);
    var stepContribution = mock(StepContribution.class);
    when(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobInstanceId)).thenReturn(100L);
    when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(90L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(10L);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.FINISHED);
  }

  @Test
  void execute_shouldReturnContinuable_givenLinesStillProcessing() throws InterruptedException {
    // given
    var jobInstanceId = 123L;
    var chunkContext = mockChunkContext(jobInstanceId);
    var stepContribution = mock(StepContribution.class);
    when(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobInstanceId)).thenReturn(100L);
    when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(50L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(5L);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.CONTINUABLE);
  }

  @ParameterizedTest
  @CsvSource({
    "100, 95, 5, true",      // 95 + 5 = 100, all processed
    "100, 100, 0, true",     // 100 + 0 = 100, all processed
    "100, 0, 100, true",     // 0 + 100 = 100, all failed
    "100, 50, 49, false",    // 50 + 49 = 99 < 100, still processing
    "100, 0, 0, false"       // 0 + 0 = 0 < 100, still processing
  })
  void execute_shouldCheckProcessingCompletion(
    long totalRead, long processedCount, long failedCount, boolean shouldFinish) throws InterruptedException {
    // given
    var jobInstanceId = 123L;
    var chunkContext = mockChunkContext(jobInstanceId);
    var stepContribution = mock(StepContribution.class);
    when(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobInstanceId)).thenReturn(totalRead);
    when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(processedCount);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(failedCount);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    var expectedStatus = shouldFinish ? RepeatStatus.FINISHED : RepeatStatus.CONTINUABLE;
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

