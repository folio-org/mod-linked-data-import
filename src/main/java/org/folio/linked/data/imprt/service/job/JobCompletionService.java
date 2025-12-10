package org.folio.linked.data.imprt.service.job;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class JobCompletionService {

  private final Map<Long, CountDownLatch> jobLatches;
  private final Map<Long, Long> expectedCounts;

  public JobCompletionService() {
    this.jobLatches = new ConcurrentHashMap<>();
    this.expectedCounts = new ConcurrentHashMap<>();
  }

  public boolean awaitCompletion(Long jobExecutionId, Long expectedLineCount, long timeout, TimeUnit timeUnit)
    throws InterruptedException {
    var latch = jobLatches.computeIfAbsent(jobExecutionId, id -> {
      log.info("Registering job execution {} with expected line count: {}", id, expectedLineCount);
      expectedCounts.put(id, expectedLineCount);
      return new CountDownLatch(1);
    });

    log.info("Waiting for job execution {} completion with timeout {} {}", jobExecutionId, timeout, timeUnit);
    return latch.await(timeout, timeUnit);
  }

  public void checkAndCompleteJob(Long jobExecutionId, Long totalProcessedCount) {
    var expectedCount = expectedCounts.get(jobExecutionId);
    if (expectedCount == null) {
      log.debug("Job execution {} is not yet registered for completion", jobExecutionId);
      return;
    }

    log.debug("Checking completion for job execution {}: processed={}, expected={}",
      jobExecutionId, totalProcessedCount, expectedCount);

    if (totalProcessedCount >= expectedCount) {
      completeJob(jobExecutionId);
    }
  }

  public void completeJob(Long jobExecutionId) {
    var latch = jobLatches.remove(jobExecutionId);
    if (latch != null) {
      log.info("Completing job execution {}", jobExecutionId);
      latch.countDown();
      expectedCounts.remove(jobExecutionId);
    } else {
      log.debug("Job execution {} already completed in parallel thread", jobExecutionId);
    }
  }
}

