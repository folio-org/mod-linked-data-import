package org.folio.linked.data.imprt.service.cleanup;

import static java.util.Objects.nonNull;

import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.repo.BatchJobExecutionRepo;
import org.folio.linked.data.imprt.repo.RdfFileLineRepo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Log4j2
@Service
@RequiredArgsConstructor
public class DataCleanupService {

  private final BatchJobExecutionRepo batchJobExecutionRepo;
  private final RdfFileLineRepo rdfFileLineRepo;

  @Value("${mod-linked-data-import.cleanup.age-days:2}")
  private int cleanupAgeDays;

  @Scheduled(cron = "${mod-linked-data-import.cleanup.cron}")
  @Transactional
  public void cleanupCompletedJobData() {
    log.info("Starting scheduled cleanup of job data older than {} days", cleanupAgeDays);

    var cutoffDate = LocalDateTime.now().minusDays(cleanupAgeDays);
    var applicableJobs = batchJobExecutionRepo.findAll().stream()
      .filter(job -> nonNull(job.getStartTime()) && job.getStartTime().isBefore(cutoffDate))
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

