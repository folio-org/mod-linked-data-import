package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.folio.linked.data.imprt.repo.BatchStepExecutionRepo;
import org.folio.linked.data.imprt.service.job.JobCompletionService;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
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
  private JobCompletionService jobCompletionService;
  @InjectMocks
  private WaitForSavingTasklet tasklet;

  @BeforeEach
  void setUp() {
    ReflectionTestUtils.setField(tasklet, "linesPerMinute", 500);
  }

  @ParameterizedTest
  @CsvSource({
    "100, true, 1",         // 100 lines -> 0.2 min calculated, rounded to 1
    "500, true, 1",         // 500 lines -> 1 min calculated
    "2500, true, 5",        // 2500 lines -> 5 min calculated
    "10000, true, 20",      // 10000 lines -> 20 min calculated
    "100, false, 1",        // timeout case
    "0, true, 1"            // no lines to process
  })
  void execute_shouldCalculateTimeoutDynamically_givenDifferentLineCounts(
    long readCount, boolean completedSuccessfully, long expectedTimeoutMinutes) throws InterruptedException {
    // given
    var jobInstanceId = 123L;
    var chunkContext = mockChunkContext(jobInstanceId);
    var stepContribution = mock(StepContribution.class);
    when(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobInstanceId)).thenReturn(readCount);

    if (readCount > 0) {
      when(jobCompletionService.awaitCompletion(eq(jobInstanceId), eq(readCount), anyLong(), any(TimeUnit.class)))
        .thenReturn(completedSuccessfully);
    }

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.FINISHED);

    if (readCount > 0) {
      var timeoutCaptor = ArgumentCaptor.forClass(Long.class);
      verify(jobCompletionService).awaitCompletion(eq(jobInstanceId), eq(readCount), timeoutCaptor.capture(),
        eq(TimeUnit.MINUTES));
      assertThat(timeoutCaptor.getValue()).isEqualTo(expectedTimeoutMinutes);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "0, 1",           // edge case: 0 lines
    "1, 1",           // 1 line -> 1 min (rounded up from 0.002)
    "100, 1",         // 100 lines -> 1 min (rounded up from 0.2)
    "499, 1",         // 499 lines -> 1 min (rounded up from 0.998)
    "500, 1",         // 500 lines -> 1 min
    "501, 2",         // 501 lines -> 2 min (rounded up from 1.002)
    "2500, 5",        // 2500 lines -> 5 min
    "3000, 6",        // 3000 lines -> 6 min
    "10000, 20",      // 10000 lines -> 20 min
    "25000, 50",      // 25000 lines -> 50 min
    "100000, 200"     // 100000 lines -> 200 min
  })
  void calculateTimeout_shouldReturnCorrectValue(long lineCount, long expectedTimeout) {
    // given
    ReflectionTestUtils.setField(tasklet, "linesPerMinute", 500);

    // when
    var timeout = (long) ReflectionTestUtils.invokeMethod(tasklet, "calculateTimeout", lineCount);

    // then
    assertThat(timeout).isEqualTo(expectedTimeout);
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

