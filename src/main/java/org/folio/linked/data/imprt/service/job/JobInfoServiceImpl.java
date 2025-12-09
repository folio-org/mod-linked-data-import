package org.folio.linked.data.imprt.service.job;

import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.STARTED_BY;

import java.util.List;
import java.util.function.ToLongFunction;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.domain.dto.JobInfo;
import org.folio.linked.data.imprt.model.entity.ImportResultEvent;
import org.folio.linked.data.imprt.repo.BatchJobExecutionParamsRepo;
import org.folio.linked.data.imprt.repo.BatchJobExecutionRepo;
import org.folio.linked.data.imprt.repo.BatchStepExecutionRepo;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.linked.data.imprt.repo.ImportResultEventRepo;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class JobInfoServiceImpl implements JobInfoService {

  private final BatchJobExecutionRepo batchJobExecutionRepo;
  private final BatchJobExecutionParamsRepo batchJobExecutionParamsRepo;
  private final BatchStepExecutionRepo batchStepExecutionRepo;
  private final ImportResultEventRepo importResultEventRepo;
  private final FailedRdfLineRepo failedRdfLineRepo;

  @Override
  public JobInfo getJobInfo(Long jobId) {
    var jobExecution = batchJobExecutionRepo.findFirstByJobInstanceIdOrderByJobExecutionIdDesc(jobId)
      .orElseThrow(() -> new IllegalArgumentException("Job execution not found for jobId: " + jobId));
    var startDate = jobExecution.getStartTime().toString();
    var endDate = jobExecution.getEndTime() != null ? jobExecution.getEndTime().toString() : null;
    var startedBy = getJobParameter(jobId, STARTED_BY);
    var fileName = getJobParameter(jobId, FILE_URL);
    var status = jobExecution.getStatus();
    var currentStep = status.isRunning()
      ? batchStepExecutionRepo.findLastStepNameByJobInstanceId(jobId).orElse(null)
      : null;
    var importResults = importResultEventRepo.findAllByJobInstanceId(jobId);
    return new JobInfo(startDate, startedBy, status.name(), fileName, currentStep)
      .endDate(endDate)
      .linesRead(batchStepExecutionRepo.getTotalReadCountByJobInstanceId(jobId))
      .linesMapped(getSum(importResults, ImportResultEvent::getResourcesCount))
      .linesFailedMapping(failedRdfLineRepo.countFailedLinesWithoutImportResultEvent(jobId))
      .linesCreated(getSum(importResults, ImportResultEvent::getCreatedCount))
      .linesUpdated(getSum(importResults, ImportResultEvent::getUpdatedCount))
      .linesFailedSaving(getSum(importResults, ire -> ire.getFailedRdfLines().size()));
  }

  private String getJobParameter(Long jobId, String parameter) {
    return batchJobExecutionParamsRepo.findByJobInstanceIdAndParameterName(jobId, parameter)
      .orElse(null);
  }

  private long getSum(List<ImportResultEvent> importResults, ToLongFunction<ImportResultEvent> valueSupplier) {
    return importResults.stream()
      .mapToLong(valueSupplier)
      .sum();
  }
}

