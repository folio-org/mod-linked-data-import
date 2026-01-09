package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.folio.linked.data.imprt.service.job.JobService;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobExecution;
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
  private JobService jobService;
  @InjectMocks
  private WaitForSavingTasklet tasklet;

  @BeforeEach
  void setUp() {
    ReflectionTestUtils.setField(tasklet, "waitIntervalMs", 100);
  }

  @Test
  void execute_shouldReturnFinished_givenNoLinesMapped() throws InterruptedException {
    // given
    var jobExecutionId = 123L;
    var chunkContext = mockChunkContext(jobExecutionId);
    var stepContribution = mock(StepContribution.class);
    when(jobService.getMappedCount(jobExecutionId)).thenReturn(0L);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.FINISHED);
  }

  @Test
  void execute_shouldReturnFinished_givenAllLinesProcessedAndSaved() throws InterruptedException {
    // given
    var jobExecutionId = 123L;
    var chunkContext = mockChunkContext(jobExecutionId);
    var stepContribution = mock(StepContribution.class);

    when(jobService.getMappedCount(jobExecutionId)).thenReturn(90L);
    when(jobService.getSavedCount(jobExecutionId)).thenReturn(90L);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.FINISHED);
  }

  @Test
  void execute_shouldReturnContinuable_givenLinesStillMapping() throws InterruptedException {
    // given
    var jobExecutionId = 123L;
    var chunkContext = mockChunkContext(jobExecutionId);
    var stepContribution = mock(StepContribution.class);

    when(jobService.getMappedCount(jobExecutionId)).thenReturn(50L);
    when(jobService.getSavedCount(jobExecutionId)).thenReturn(0L);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.CONTINUABLE);
  }

  @Test
  void execute_shouldReturnContinuable_givenLinesMappedButNotSaved() throws InterruptedException {
    // given
    var jobExecutionId = 123L;
    var chunkContext = mockChunkContext(jobExecutionId);
    var stepContribution = mock(StepContribution.class);

    when(jobService.getMappedCount(jobExecutionId)).thenReturn(90L);
    when(jobService.getSavedCount(jobExecutionId)).thenReturn(0L);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.CONTINUABLE);
  }

  @Test
  void execute_shouldReturnContinuable_givenPartialSaving() throws InterruptedException {
    // given
    var jobExecutionId = 123L;
    var chunkContext = mockChunkContext(jobExecutionId);
    var stepContribution = mock(StepContribution.class);

    when(jobService.getMappedCount(jobExecutionId)).thenReturn(90L);
    when(jobService.getSavedCount(jobExecutionId)).thenReturn(40L);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.CONTINUABLE);
  }

  @Test
  void execute_shouldReturnFinished_givenJobIsStopping() throws InterruptedException {
    // given
    var jobExecutionId = 123L;
    var chunkContext = mockChunkContext(jobExecutionId);
    var stepContribution = mock(StepContribution.class);
    var jobExecution = chunkContext.getStepContext().getStepExecution().getJobExecution();

    when(jobExecution.isStopping()).thenReturn(true);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.FINISHED);
  }

  private ChunkContext mockChunkContext(Long jobExecutionId) {
    var chunkContext = mock(ChunkContext.class);
    var stepContext = mock(StepContext.class);
    var stepExecution = mock(StepExecution.class);
    var jobExecution = mock(JobExecution.class);
    var executionContext = new ExecutionContext();

    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobExecution()).thenReturn(jobExecution);
    lenient().when(stepExecution.getExecutionContext()).thenReturn(executionContext);
    when(jobExecution.getId()).thenReturn(jobExecutionId);
    return chunkContext;
  }
}

