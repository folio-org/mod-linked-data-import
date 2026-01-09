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
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Log4j2
@Service
@RequiredArgsConstructor
public class JobServiceImpl implements JobService {

  private static final CSVFormat FORMAT = CSVFormat.EXCEL.builder()
    .setHeader("lineNumber", "description", "failedRdfLine")
    .setRecordSeparator("\n")
    .get();
  private final BatchJobExecutionRepo batchJobExecutionRepo;
  private final BatchJobExecutionParamsRepo batchJobExecutionParamsRepo;
  private final BatchStepExecutionRepo batchStepExecutionRepo;
  private final ImportResultEventRepo importResultEventRepo;
  private final FailedRdfLineRepo failedRdfLineRepo;
  private final JobOperator jobOperator;
  private final JobExplorer jobExplorer;

  @Override
  public JobInfo getJobInfo(Long jobExecutionId) {
    var jobExecution = batchJobExecutionRepo.findByJobExecutionId(jobExecutionId)
      .orElseThrow(() -> new IllegalArgumentException("Job execution not found for jobExecutionId: " + jobExecutionId));
    var startDate = jobExecution.getStartTime().toString();
    var endDate = jobExecution.getEndTime() != null ? jobExecution.getEndTime().toString() : null;
    var startedBy = getJobParameter(jobExecutionId, STARTED_BY);
    var fileName = getJobParameter(jobExecutionId, FILE_URL);
    var status = jobExecution.getStatus();
    var currentStep = batchStepExecutionRepo.findLastStepNameByJobExecutionId(jobExecutionId).orElse(null);
    var importResults = importResultEventRepo.findAllByJobExecutionId(jobExecutionId);
    return new JobInfo(startDate, startedBy, status.name(), fileName, currentStep)
      .endDate(endDate)
      .linesRead(batchStepExecutionRepo.getTotalReadCountByJobExecutionId(jobExecutionId))
      .linesMapped(getMappedCount(jobExecutionId))
      .linesFailedMapping(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobExecutionId))
      .linesCreated(getCreatedCount(importResults))
      .linesUpdated(getUpdatedCount(importResults))
      .linesFailedSaving(getFailedSavingCount(importResults));
  }

  @Override
  public Long getMappedCount(Long jobExecutionId) {
    return batchStepExecutionRepo.getMappedCountByJobExecutionId(jobExecutionId);
  }

  @Override
  public long getSavedCount(Long jobExecutionId) {
    var importResults = importResultEventRepo.findAllByJobExecutionId(jobExecutionId);
    return getCreatedCount(importResults) + getUpdatedCount(importResults) + getFailedSavingCount(importResults);
  }

  @Override
  @Transactional
  public Resource generateFailedLinesCsv(Long jobExecutionId) {
    try (var writer = new StringWriter();
         var csvPrinter = new CSVPrinter(writer, FORMAT);
         var failedLines = failedRdfLineRepo.findAllByJobExecutionIdOrderByLineNumber(jobExecutionId)) {
      failedLines.forEach(line -> {
        try {
          csvPrinter.printRecord(line.getLineNumber(), line.getDescription(), line.getFailedRdfLine());
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to printRecord for jobExecutionId: " + jobExecutionId, e);
        }
      });
      csvPrinter.flush();
      return new ByteArrayResource(writer.toString().getBytes(UTF_8));
    } catch (IOException e) {
      log.error("Error generating CSV for jobExecutionId={}", jobExecutionId, e);
      throw new UncheckedIOException("Failed to generate CSV for jobExecutionId: " + jobExecutionId, e);
    }
  }

  @Override
  public void cancelJob(Long jobExecutionId) {
    try {
      var jobExecution = jobExplorer.getJobExecution(jobExecutionId);
      if (jobExecution == null) {
        throw new IllegalArgumentException("Job execution not found for jobExecutionId: " + jobExecutionId);
      }
      var status = jobExecution.getStatus();
      if (!status.isRunning()) {
        throw new IllegalStateException(
          "Job execution " + jobExecutionId + " is not running. Current status: " + status
        );
      }
      jobOperator.stop(jobExecutionId);
      log.info("Job execution {} has been stopped", jobExecutionId);
    } catch (NoSuchJobExecutionException e) {
      throw new IllegalArgumentException("Job execution not found for jobExecutionId: " + jobExecutionId, e);
    } catch (org.springframework.batch.core.launch.JobExecutionNotRunningException e) {
      throw new IllegalStateException("Job execution " + jobExecutionId + " is not running", e);
    }
  }

  private String getJobParameter(Long jobExecutionId, String parameter) {
    return batchJobExecutionParamsRepo.findByJobExecutionIdAndParameterName(jobExecutionId, parameter)
      .orElse(null);
  }

  private long getCreatedCount(List<ImportResultEvent> importResults) {
    return getSum(importResults, ImportResultEvent::getCreatedCount);
  }

  private long getUpdatedCount(List<ImportResultEvent> importResults) {
    return getSum(importResults, ImportResultEvent::getUpdatedCount);
  }

  private long getFailedSavingCount(List<ImportResultEvent> importResults) {
    return getSum(importResults, ire -> ire.getFailedRdfLines().size());
  }

  private long getSum(List<ImportResultEvent> importResults, ToLongFunction<ImportResultEvent> valueSupplier) {
    return importResults.stream()
      .mapToLong(valueSupplier)
      .sum();
  }
}

