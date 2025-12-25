package org.folio.linked.data.imprt.integration.kafka.handler;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEvent;
import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEventDto;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;
import org.folio.linked.data.imprt.model.entity.FailedRdfLine;
import org.folio.linked.data.imprt.model.mapper.ImportResultEventMapper;
import org.folio.linked.data.imprt.repo.BatchJobExecutionParamsRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.file.FileService;
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
  private FileService fileService;

  @Mock
  private ImportResultEventRepo importResultEventRepo;

  @Mock
  private ImportResultEventMapper importResultEventMapper;

  @Mock
  private BatchJobExecutionParamsRepo batchJobExecutionParamsRepo;


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
    verifyNoInteractions(fileService, batchJobExecutionParamsRepo);
  }

  @Test
  void handle_shouldReadFailedLinesFromFileAndSaveEntity() {
    // given
    var jobExecutionId = 123L;
    var dto = createImportResultEventDto(jobExecutionId);
    var entity = createImportResultEvent(jobExecutionId);
    var failedLine1 = new FailedRdfLine().setId(1L).setLineNumber(5L);
    var failedLine2 = new FailedRdfLine().setId(2L).setLineNumber(10L);
    entity.setFailedRdfLines(Set.of(failedLine1, failedLine2));
    var fileUrl = "s3://bucket/test-file.txt";
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL))
      .thenReturn(Optional.of(fileUrl));
    when(fileService.readLineFromFile(eq(fileUrl), anyLong())).thenReturn("RDF line content");

    // when
    handler.handle(dto);

    // then
    verify(fileService, times(2)).readLineFromFile(eq(fileUrl), anyLong());
    verify(importResultEventRepo).save(entity);
  }

  @Test
  void handle_shouldSetErrorMessageWhenFileUrlNotFound() {
    // given
    var jobExecutionId = 123L;
    var dto = createImportResultEventDto(jobExecutionId);
    var entity = createImportResultEvent(jobExecutionId);
    var failedLine = new FailedRdfLine().setId(1L).setLineNumber(5L);
    entity.setFailedRdfLines(Set.of(failedLine));
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL))
      .thenReturn(Optional.empty());

    // when
    handler.handle(dto);

    // then
    verify(importResultEventRepo).save(entity);
    verifyNoInteractions(fileService);
  }

  @Test
  void handle_shouldHandleFileServiceReturningNull() {
    // given
    var jobExecutionId = 123L;
    var dto = createImportResultEventDto(jobExecutionId);
    var entity = createImportResultEvent(jobExecutionId);
    var failedLine = new FailedRdfLine().setId(1L).setLineNumber(999L);
    entity.setFailedRdfLines(Set.of(failedLine));
    var fileUrl = "s3://bucket/test-file.txt";
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL))
      .thenReturn(Optional.of(fileUrl));
    when(fileService.readLineFromFile(fileUrl, 999L)).thenReturn(null);

    // when
    handler.handle(dto);

    // then
    verify(fileService).readLineFromFile(fileUrl, 999L);
    verify(importResultEventRepo).save(entity);
  }

  @Test
  void handle_shouldReadFailedLinesWhenFileUrlExists() {
    // given
    var jobExecutionId = 123L;
    var dto = createImportResultEventDto(jobExecutionId);
    var entity = createImportResultEvent(jobExecutionId);
    var failedLine = new FailedRdfLine().setId(1L).setLineNumber(1L);
    entity.setFailedRdfLines(Set.of(failedLine));
    var fileUrl = "s3://bucket/test-file.txt";
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL))
      .thenReturn(Optional.of(fileUrl));
    when(fileService.readLineFromFile(eq(fileUrl), anyLong())).thenReturn("test content");

    // when
    handler.handle(dto);

    // then
    verify(batchJobExecutionParamsRepo).findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL);
    verify(fileService).readLineFromFile(eq(fileUrl), anyLong());
    verify(importResultEventRepo).save(entity);
  }

  @Test
  void handle_shouldUseErrorMessageWhenFileUrlDoesNotExist() {
    // given
    var jobExecutionId = 123L;
    var dto = createImportResultEventDto(jobExecutionId);
    var entity = createImportResultEvent(jobExecutionId);
    var failedLine = new FailedRdfLine().setId(1L).setLineNumber(1L);
    entity.setFailedRdfLines(Set.of(failedLine));
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL))
      .thenReturn(Optional.empty());

    // when
    handler.handle(dto);

    // then
    verify(batchJobExecutionParamsRepo).findByJobExecutionIdAndParameterName(jobExecutionId, FILE_URL);
    verify(importResultEventRepo).save(entity);
    verifyNoInteractions(fileService);
  }
}

