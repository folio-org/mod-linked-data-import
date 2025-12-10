package org.folio.linked.data.imprt.integration.kafka.handler;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEvent;
import static org.folio.linked.data.imprt.test.TestUtil.createImportResultEventDto;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;
import org.folio.linked.data.imprt.model.entity.FailedRdfLine;
import org.folio.linked.data.imprt.model.mapper.ImportResultEventMapper;
import org.folio.linked.data.imprt.repo.BatchJobExecutionParamsRepo;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.folio.linked.data.imprt.service.file.FileService;
import org.folio.linked.data.imprt.service.job.JobCompletionService;
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
  private FailedRdfLineRepo failedRdfLineRepo;

  @Mock
  private ImportResultEventMapper importResultEventMapper;

  @Mock
  private BatchJobExecutionParamsRepo batchJobExecutionParamsRepo;

  @Mock
  private JobCompletionService jobCompletionService;

  @Test
  void handle_shouldSaveEntityWithoutFailedLines() {
    // given
    var jobInstanceId = 123L;
    var dto = createImportResultEventDto(jobInstanceId);
    var entity = createImportResultEvent(jobInstanceId);
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(10L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(0L);

    // when
    handler.handle(dto);

    // then
    verify(importResultEventRepo).save(entity);
    verify(jobCompletionService).checkAndCompleteJob(jobInstanceId, 10L);
  }

  @Test
  void handle_shouldReadFailedLinesFromFileAndSaveEntity() {
    // given
    var jobInstanceId = 123L;
    var dto = createImportResultEventDto(jobInstanceId);
    var entity = createImportResultEvent(jobInstanceId);
    var failedLine1 = new FailedRdfLine().setId(1L).setLineNumber(5L);
    var failedLine2 = new FailedRdfLine().setId(2L).setLineNumber(10L);
    entity.setFailedRdfLines(Set.of(failedLine1, failedLine2));
    var fileUrl = "s3://bucket/test-file.txt";
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobInstanceId, FILE_URL))
      .thenReturn(Optional.of(fileUrl));
    when(fileService.readLineFromFile(eq(fileUrl), anyLong())).thenReturn("RDF line content");
    when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(20L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(5L);

    // when
    handler.handle(dto);

    // then
    verify(fileService, times(2)).readLineFromFile(eq(fileUrl), anyLong());
    verify(importResultEventRepo).save(entity);
    verify(jobCompletionService).checkAndCompleteJob(jobInstanceId, 25L);
  }

  @Test
  void handle_shouldSetErrorMessageWhenFileUrlNotFound() {
    // given
    var jobInstanceId = 123L;
    var dto = createImportResultEventDto(jobInstanceId);
    var entity = createImportResultEvent(jobInstanceId);
    var failedLine = new FailedRdfLine().setId(1L).setLineNumber(5L);
    entity.setFailedRdfLines(Set.of(failedLine));
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobInstanceId, FILE_URL))
      .thenReturn(Optional.empty());
    when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(0L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(0L);

    // when
    handler.handle(dto);

    // then
    verify(importResultEventRepo).save(entity);
    verify(jobCompletionService).checkAndCompleteJob(jobInstanceId, 0L);
  }

  @Test
  void handle_shouldHandleNullLineContentFromFile() {
    // given
    var jobInstanceId = 123L;
    var dto = createImportResultEventDto(jobInstanceId);
    var entity = createImportResultEvent(jobInstanceId);
    var failedLine = new FailedRdfLine().setId(1L).setLineNumber(999L);
    entity.setFailedRdfLines(Set.of(failedLine));
    var fileUrl = "s3://bucket/test-file.txt";
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobInstanceId, FILE_URL))
      .thenReturn(Optional.of(fileUrl));
    when(fileService.readLineFromFile(fileUrl, 999L)).thenReturn(null);
    when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(5L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(2L);

    // when
    handler.handle(dto);

    // then
    verify(fileService).readLineFromFile(fileUrl, 999L);
    verify(importResultEventRepo).save(entity);
    verify(jobCompletionService).checkAndCompleteJob(jobInstanceId, 7L);
  }

  @Test
  void getFileUrl_shouldReturnFileUrlWhenParameterExists() {
    // given
    var jobInstanceId = 123L;
    var fileUrl = "s3://bucket/test-file.txt";
    var dto = createImportResultEventDto(jobInstanceId);
    var entity = createImportResultEvent(jobInstanceId);
    var failedLine = new FailedRdfLine().setId(1L).setLineNumber(1L);
    entity.setFailedRdfLines(Set.of(failedLine));
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobInstanceId, FILE_URL))
      .thenReturn(Optional.of(fileUrl));
    when(fileService.readLineFromFile(eq(fileUrl), anyLong())).thenReturn("test content");
    when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(100L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(10L);

    // when
    handler.handle(dto);

    // then
    verify(batchJobExecutionParamsRepo).findByJobInstanceIdAndParameterName(jobInstanceId, FILE_URL);
    verify(fileService).readLineFromFile(eq(fileUrl), anyLong());
    verify(jobCompletionService).checkAndCompleteJob(jobInstanceId, 110L);
  }

  @Test
  void getFileUrl_shouldReturnEmptyWhenParameterDoesNotExist() {
    // given
    var jobInstanceId = 123L;
    var dto = createImportResultEventDto(jobInstanceId);
    var entity = createImportResultEvent(jobInstanceId);
    var failedLine = new FailedRdfLine().setId(1L).setLineNumber(1L);
    entity.setFailedRdfLines(Set.of(failedLine));
    when(importResultEventMapper.toEntity(dto)).thenReturn(entity);
    when(batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobInstanceId, FILE_URL))
      .thenReturn(Optional.empty());
    when(importResultEventRepo.getTotalResourcesCountByJobInstanceId(jobInstanceId)).thenReturn(50L);
    when(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobInstanceId)).thenReturn(5L);

    // when
    handler.handle(dto);

    // then
    verify(batchJobExecutionParamsRepo).findByJobInstanceIdAndParameterName(jobInstanceId, FILE_URL);
    verify(jobCompletionService).checkAndCompleteJob(jobInstanceId, 55L);
  }
}

