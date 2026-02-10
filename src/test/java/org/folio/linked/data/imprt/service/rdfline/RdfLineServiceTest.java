package org.folio.linked.data.imprt.service.rdfline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.folio.linked.data.imprt.model.entity.RdfFileLine;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class RdfLineServiceTest {

  @Mock
  private RdfFileLineRepo rdfFileLineRepo;

  @InjectMocks
  private RdfLineServiceImpl rdfLineService;

  @Test
  void readLineContent_shouldReturnContent_whenLineExists() {
    // given
    var jobExecutionId = 123L;
    var lineNumber = 3L;
    var expectedContent = "Test RDF content line 3";
    var rdfLine = new RdfFileLine()
      .setJobExecutionId(jobExecutionId)
      .setLineNumber(lineNumber)
      .setContent(expectedContent);

    when(rdfFileLineRepo.findByJobExecutionIdAndLineNumber(jobExecutionId, lineNumber))
      .thenReturn(Optional.of(rdfLine));

    // when
    var result = rdfLineService.readLineContent(jobExecutionId, lineNumber);

    // then
    assertThat(result).isEqualTo(expectedContent);
  }

  @Test
  void readLineContent_shouldReturnNotFoundMessage_whenLineDoesNotExist() {
    // given
    var jobExecutionId = 123L;
    var lineNumber = 999L;

    when(rdfFileLineRepo.findByJobExecutionIdAndLineNumber(jobExecutionId, lineNumber))
      .thenReturn(Optional.empty());

    // when
    var result = rdfLineService.readLineContent(jobExecutionId, lineNumber);

    // then
    assertThat(result).contains("Line number 999 not found");
  }

  @Test
  void readLineContent_shouldReturnNotFoundMessage_whenNoLinesExist() {
    // given
    var jobExecutionId = 123L;
    var lineNumber = 1L;

    when(rdfFileLineRepo.findByJobExecutionIdAndLineNumber(jobExecutionId, lineNumber))
      .thenReturn(Optional.empty());

    // when
    var result = rdfLineService.readLineContent(jobExecutionId, lineNumber);

    // then
    assertThat(result).contains("Line number 1 not found");
  }
}

