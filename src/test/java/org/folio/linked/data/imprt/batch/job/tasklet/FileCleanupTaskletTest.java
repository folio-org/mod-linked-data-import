package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.repeat.RepeatStatus;

@UnitTest
class FileCleanupTaskletTest {

  private final RdfFileLineRepo rdfFileLineRepo = mock(RdfFileLineRepo.class);
  private final FileCleanupTasklet tasklet = new FileCleanupTasklet(rdfFileLineRepo);

  @Test
  void shouldDeleteRdfFileLinesFromDatabase_givenJobExecutionId() {
    // given
    var jobExecutionId = 123L;
    var chunkContext = mockChunkContext(jobExecutionId);
    var stepContribution = mock(StepContribution.class);

    // when
    var result = tasklet.execute(stepContribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.FINISHED);
    verify(rdfFileLineRepo).deleteByJobExecutionId(jobExecutionId);
  }

  private ChunkContext mockChunkContext(Long jobExecutionId) {
    var chunkContext = mock(ChunkContext.class);
    var stepContext = mock(StepContext.class);
    var stepExecution = mock(StepExecution.class);
    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobExecutionId()).thenReturn(jobExecutionId);
    return chunkContext;
  }
}
