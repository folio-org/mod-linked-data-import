package org.folio.linked.data.imprt.service.cleanup;

import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.repo.BatchJobExecutionRepo;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.springframework.batch.core.BatchStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Log4j2
@Service
@RequiredArgsConstructor
public class DataCleanupService {

  private static final Set<BatchStatus> STATUSES_FOR_CLEANUP = Set.of(
    BatchStatus.ABANDONED,
    BatchStatus.COMPLETED,
    BatchStatus.FAILED,
    BatchStatus.STOPPED
  );

  private final BatchJobExecutionRepo batchJobExecutionRepo;
  private final RdfFileLineRepo rdfFileLineRepo;

  @Scheduled(cron = "${mod-linked-data-import.cleanup.cron}")
  @Transactional
  public void cleanupCompletedJobData() {
    log.info("Starting scheduled cleanup of completed job data");

    var applicableJobs = batchJobExecutionRepo.findAll().stream()
      .filter(job -> STATUSES_FOR_CLEANUP.contains(job.getStatus()))
      .toList();
    if (applicableJobs.isEmpty()) {
      log.info("No applicable jobs found for cleanup");
      return;
    }
    var totalCleaned = applicableJobs.stream()
      .mapToLong(job -> cleanupJobData(job.getJobExecutionId()))
      .sum();

    log.info("Cleanup completed. {} line(s) cleaned for {} jobs", totalCleaned, applicableJobs.size());
  }

  private long cleanupJobData(Long jobExecutionId) {
    log.debug("Cleaning up data for job execution: {}", jobExecutionId);
    try {
      var deletedCount = rdfFileLineRepo.deleteByJobExecutionId(jobExecutionId);
      log.debug("Successfully cleaned up data for job execution: {}", jobExecutionId);
      return deletedCount;
    } catch (Exception e) {
      log.error("Error cleaning up data for job execution: {}", jobExecutionId, e);
      return 0L;
    }
  }
}

