package org.folio.linked.data.imprt.integration.kafka.handler;

import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEvent;
import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEventDto;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;
import org.folio.linked.data.imprt.model.entity.FailedRdfLine;
import org.folio.linked.data.imprt.model.mapper.ImportResultEventMapper;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.rdfline.RdfLineService;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class ImportResultEventHandlerTest {

  @InjectMocks
  private ImportResultEventHandler handler;

  @Mock
  private RdfLineService rdfLineService;

  @Mock
  private ImportResultEventRepo importResultEventRepo;

  @Mock
  private ImportResultEventMapper importResultEventMapper;



  @Test
  void handle_shouldSaveEntityWithoutFailedLines() {
    // given
    var jobExecutionId = 123L;
    var dto = createImportResultEventDto(jobExecutionId);
    var entity = createImportResultEvent(jobExecutionId);
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);

    // when
    handler.handle(dto);

    // then
    verify(importResultEventRepo).save(entity);
    verifyNoInteractions(rdfLineService);
  }

  @Test
  void handle_shouldReadFailedLinesAndSaveEntity() {
    // given
    var jobExecutionId = 123L;
    var dto = createImportResultEventDto(jobExecutionId);
    var entity = createImportResultEvent(jobExecutionId);
    var failedLine1 = new FailedRdfLine().setId(1L).setLineNumber(5L);
    var failedLine2 = new FailedRdfLine().setId(2L).setLineNumber(10L);
    entity.setFailedRdfLines(Set.of(failedLine1, failedLine2));
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(rdfLineService.readLineContent(eq(jobExecutionId), anyLong())).thenReturn("RDF line content");

    // when
    handler.handle(dto);

    // then
    verify(rdfLineService, times(2)).readLineContent(eq(jobExecutionId), anyLong());
    verify(importResultEventRepo).save(entity);
  }

  @Test
  void handle_shouldHandleRdfLineServiceReturningNull() {
    // given
    var jobExecutionId = 123L;
    var dto = createImportResultEventDto(jobExecutionId);
    var entity = createImportResultEvent(jobExecutionId);
    var failedLine = new FailedRdfLine().setId(1L).setLineNumber(999L);
    entity.setFailedRdfLines(Set.of(failedLine));
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(rdfLineService.readLineContent(jobExecutionId, 999L)).thenReturn(null);

    // when
    handler.handle(dto);

    // then
    verify(rdfLineService).readLineContent(jobExecutionId, 999L);
    verify(importResultEventRepo).save(entity);
  }
}

