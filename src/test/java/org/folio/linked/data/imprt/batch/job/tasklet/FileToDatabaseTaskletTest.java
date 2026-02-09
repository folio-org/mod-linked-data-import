package org.folio.linked.data.imprt.batch.job.tasklet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_NAME;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileWriter;
import org.folio.linked.data.imprt.model.entity.RdfFileLine;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.repeat.RepeatStatus;

@UnitTest
@ExtendWith(MockitoExtension.class)
class FileToDatabaseTaskletTest {

  @Mock
  private RdfFileLineRepo rdfFileLineRepo;
  @Mock
  private ChunkContext chunkContext;
  @Mock
  private StepContext stepContext;
  @Mock
  private StepExecution stepExecution;
  @Mock
  private StepContribution contribution;
  @Mock
  private JobParameters jobParameters;
  @Captor
  private ArgumentCaptor<java.util.List<RdfFileLine>> linesCaptor;
  @InjectMocks
  private FileToDatabaseTasklet tasklet;

  @Test
  void execute_shouldSaveFileToDatabaseAndDeleteFile() throws Exception {
    // given
    var fileName = "test.rdf";
    var tmpDir = System.getProperty("java.io.tmpdir");
    var testFile = new File(tmpDir, fileName);
    try (var writer = new FileWriter(testFile)) {
      writer.write("line1\n");
      writer.write("line2\n");
      writer.write("line3\n");
    }

    var jobExecutionId = 123L;

    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobExecutionId()).thenReturn(jobExecutionId);
    when(stepExecution.getJobParameters()).thenReturn(jobParameters);
    when(jobParameters.getString(FILE_NAME)).thenReturn(fileName);

    // when
    var result = tasklet.execute(contribution, chunkContext);

    // then
    assertThat(result).isEqualTo(RepeatStatus.FINISHED);
    verify(rdfFileLineRepo, times(1)).saveAll(linesCaptor.capture());

    var savedLines = linesCaptor.getValue();
    assertThat(savedLines).hasSize(3);
    assertThat(savedLines.get(0).getJobExecutionId()).isEqualTo(jobExecutionId);
    assertThat(savedLines.get(0).getLineNumber()).isEqualTo(1L);
    assertThat(savedLines.get(0).getContent()).isEqualTo("line1");
    assertThat(savedLines.get(1).getLineNumber()).isEqualTo(2L);
    assertThat(savedLines.get(1).getContent()).isEqualTo("line2");
    assertThat(savedLines.get(2).getLineNumber()).isEqualTo(3L);
    assertThat(savedLines.get(2).getContent()).isEqualTo("line3");

    assertThat(testFile).doesNotExist();
  }

  @Test
  void execute_shouldThrowException_whenFileNameIsNull() {
    // given
    when(chunkContext.getStepContext()).thenReturn(stepContext);
    when(stepContext.getStepExecution()).thenReturn(stepExecution);
    when(stepExecution.getJobParameters()).thenReturn(jobParameters);
    when(jobParameters.getString(FILE_NAME)).thenReturn(null);

    // when & then
    assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("File URL parameter is required");
  }
}
