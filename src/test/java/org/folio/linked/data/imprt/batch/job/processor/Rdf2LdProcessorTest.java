package org.folio.linked.data.imprt.batch.job.processor;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.util.Set;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.model.RdfLineWithNumber;
import org.folio.linked.data.imprt.model.entity.FailedRdfLine;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.rdf4ld.service.Rdf4LdService;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class Rdf2LdProcessorTest {

  private static final String CONTENT_TYPE = "contentType";
  private static final long JOB_EXECUTION_ID = 1L;
  private Rdf2LdProcessor rdf2LdProcessor;
  @Mock
  private Rdf4LdService rdf4LdService;
  @Mock
  private FailedRdfLineRepo failedRdfLineRepo;

  @BeforeEach
  void setUp() {
    // jobExecutionId передается напрямую в конструктор для тестов
    rdf2LdProcessor = new Rdf2LdProcessor(JOB_EXECUTION_ID, CONTENT_TYPE, rdf4LdService, failedRdfLineRepo);
  }

  @Test
  void process_shouldReturnSuccessfulRdf4LdServiceResult() {
    // given
    var lineNumber = 1L;
    var rdfLine = new RdfLineWithNumber(lineNumber, "rdfLine");
    var resource = new Resource().setId(JOB_EXECUTION_ID);
    var rdf4LdResult = Set.of(resource);
    doReturn(rdf4LdResult).when(rdf4LdService).mapBibframe2RdfToLd(any(), eq(CONTENT_TYPE));

    // when
    var result = rdf2LdProcessor.process(rdfLine);

    // then
    assertThat(result).hasSize(1);
    var resourceWithLineNumber = result.iterator().next();
    assertThat(resourceWithLineNumber.getLineNumber()).isEqualTo(lineNumber);
    assertThat(resourceWithLineNumber.getResource()).isEqualTo(resource);
  }

  @Test
  void process_shouldSaveFailedRdfLineAndReturnNull_ifAnyExceptionInRdf4LdService() {
    // given
    var rdfLine = new RdfLineWithNumber(2L, "rdfLine");
    var rdf4LdException = "rdf4LdService exception";
    doThrow(new RuntimeException(rdf4LdException)).when(rdf4LdService).mapBibframe2RdfToLd(any(), eq(CONTENT_TYPE));
    var expectedFailedRdfLine = new FailedRdfLine()
      .setJobExecutionId(JOB_EXECUTION_ID)
      .setLineNumber(rdfLine.getLineNumber())
      .setFailedRdfLine(rdfLine.getContent())
      .setDescription(rdf4LdException);

    // when
    var result = rdf2LdProcessor.process(rdfLine);

    // then
    assertThat(result).isNull();
    verify(failedRdfLineRepo).save(expectedFailedRdfLine);
  }

  @Test
  void process_shouldSaveFailedRdfLineAndReturnNull_ifEmptyResultFromRdf4LdService() {
    // given
    var rdfLine = new RdfLineWithNumber(3L, "rdfLine");
    var expectedResult = Set.of();
    doReturn(expectedResult).when(rdf4LdService).mapBibframe2RdfToLd(any(), eq(CONTENT_TYPE));
    var expectedFailedRdfLine = new FailedRdfLine()
      .setJobExecutionId(JOB_EXECUTION_ID)
      .setLineNumber(rdfLine.getLineNumber())
      .setFailedRdfLine(rdfLine.getContent())
      .setDescription("Empty result returned by rdf4ld library");

    // when
    var result = rdf2LdProcessor.process(rdfLine);

    // then
    assertThat(result).isNull();
    verify(failedRdfLineRepo).save(expectedFailedRdfLine);
  }
}
