package org.folio.linked.data.imprt.batch.job.processor;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

import java.util.Set;
import org.folio.ld.dictionary.model.Resource;
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
  private Rdf2LdProcessor rdf2LdProcessor;
  @Mock
  private Rdf4LdService rdf4LdService;

  @BeforeEach
  void setUp() {
    rdf2LdProcessor = new Rdf2LdProcessor(CONTENT_TYPE, rdf4LdService);
  }

  @Test
  void process_shouldReturnRdf4LdServiceResult() {
    // given
    var rdfLine = "rdfLine";
    var expectedResult = Set.of(new Resource().setId(1L));
    doReturn(expectedResult).when(rdf4LdService).mapBibframe2RdfToLd(any(), eq(CONTENT_TYPE));

    // when
    var result = rdf2LdProcessor.process(rdfLine);

    // then
    assertThat(result).isEqualTo(expectedResult);
  }
}
