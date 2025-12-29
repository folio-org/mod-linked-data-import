package org.folio.linked.data.imprt.service.job;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.STARTED_BY;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.ToLongFunction;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.folio.linked.data.imprt.domain.dto.JobInfo;
import org.folio.linked.data.imprt.model.entity.ImportResultEvent;
import org.folio.linked.data.imprt.repo.BatchJobExecutionParamsRepo;
import org.folio.linked.data.imprt.repo.BatchJobExecutionRepo;
import org.folio.linked.data.imprt.repo.BatchStepExecutionRepo;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Log4j2
@Service
@RequiredArgsConstructor
public class JobInfoServiceImpl implements JobInfoService {

  private static final CSVFormat FORMAT = CSVFormat.EXCEL.builder()
    .setHeader("lineNumber", "description", "failedRdfLine")
    .setRecordSeparator("\n")
    .get();
  private final BatchJobExecutionRepo batchJobExecutionRepo;
  private final BatchJobExecutionParamsRepo batchJobExecutionParamsRepo;
  private final BatchStepExecutionRepo batchStepExecutionRepo;
  private final ImportResultEventRepo importResultEventRepo;
  private final FailedRdfLineRepo failedRdfLineRepo;

  @Override
  public JobInfo getJobInfo(Long jobId) {
    var jobExecution = batchJobExecutionRepo.findByJobExecutionId(jobId)
      .orElseThrow(() -> new IllegalArgumentException("Job execution not found for jobExecutionId: " + jobId));
    var startDate = jobExecution.getStartTime().toString();
    var endDate = jobExecution.getEndTime() != null ? jobExecution.getEndTime().toString() : null;
    var startedBy = getJobParameter(jobId, STARTED_BY);
    var fileName = getJobParameter(jobId, FILE_URL);
    var status = jobExecution.getStatus();
    var currentStep = status.isRunning()
      ? batchStepExecutionRepo.findLastStepNameByJobExecutionId(jobId).orElse(null)
      : null;
    var importResults = importResultEventRepo.findAllByJobExecutionId(jobId);
    return new JobInfo(startDate, startedBy, status.name(), fileName, currentStep)
      .endDate(endDate)
      .linesRead(batchStepExecutionRepo.getTotalReadCountByJobExecutionId(jobId))
      .linesMapped(batchStepExecutionRepo.getTotalWriteCountByJobExecutionId(jobId))
      .linesFailedMapping(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobId))
      .linesCreated(getSum(importResults, ImportResultEvent::getCreatedCount))
      .linesUpdated(getSum(importResults, ImportResultEvent::getUpdatedCount))
      .linesFailedSaving(getSum(importResults, ire -> ire.getFailedRdfLines().size()));
  }

  private String getJobParameter(Long jobId, String parameter) {
    return batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobId, parameter)
      .orElse(null);
  }

  private long getSum(List<ImportResultEvent> importResults, ToLongFunction<ImportResultEvent> valueSupplier) {
    return importResults.stream()
      .mapToLong(valueSupplier)
      .sum();
  }

  @Override
  @Transactional
  public Resource generateFailedLinesCsv(Long jobId) {
    try (var writer = new StringWriter();
         var csvPrinter = new CSVPrinter(writer, FORMAT);
         var failedLines = failedRdfLineRepo.findAllByJobExecutionIdOrderByLineNumber(jobId)) {
      failedLines.forEach(line -> {
        try {
          csvPrinter.printRecord(line.getLineNumber(), line.getDescription(), line.getFailedRdfLine());
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to printRecord for jobId: " + jobId, e);
        }
      });
      csvPrinter.flush();
      return new ByteArrayResource(writer.toString().getBytes(UTF_8));
    } catch (IOException e) {
      log.error("Error generating CSV for jobId={}", jobId, e);
      throw new UncheckedIOException("Failed to generate CSV for jobId: " + jobId, e);
    }
  }

}

