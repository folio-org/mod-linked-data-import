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

  private final Map<Long, CountDownLatch> jobLatches = new ConcurrentHashMap<>();
  private final Map<Long, Long> expectedCounts = new ConcurrentHashMap<>();

  public boolean awaitCompletion(Long jobInstanceId, Long expectedLineCount, long timeout, TimeUnit timeUnit)
    throws InterruptedException {
    var latch = jobLatches.computeIfAbsent(jobInstanceId, id -> {
      log.info("Registering job {} with expected line count: {}", id, expectedLineCount);
      expectedCounts.put(id, expectedLineCount);
      return new CountDownLatch(1);
    });

    log.info("Waiting for job {} completion with timeout {} {}", jobInstanceId, timeout, timeUnit);
    return latch.await(timeout, timeUnit);
  }

  public void checkAndCompleteJob(Long jobInstanceId, Long totalProcessedCount) {
    var expectedCount = expectedCounts.get(jobInstanceId);
    if (expectedCount == null) {
      log.debug("Job instance {} is not yet registered for completion", jobInstanceId);
      return;
    }

    log.debug("Checking completion for job {}: processed={}, expected={}",
      jobInstanceId, totalProcessedCount, expectedCount);

    if (totalProcessedCount >= expectedCount) {
      completeJob(jobInstanceId);
    }
  }

  public void completeJob(Long jobInstanceId) {
    var latch = jobLatches.remove(jobInstanceId);
    if (latch != null) {
      log.info("Completing job {}", jobInstanceId);
      latch.countDown();
      expectedCounts.remove(jobInstanceId);
    } else {
      log.debug("Job {} already completed in parallel thread", jobInstanceId);
    }
  }
}

