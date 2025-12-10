package org.folio.linked.data.imprt.service.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@UnitTest
class JobCompletionServiceTest {

  private final JobCompletionService service = new JobCompletionService();

  @ParameterizedTest
  @CsvSource({
    "1, 10, 10, true",    // count matches expected
    "2, 10, 15, true",    // count exceeds expected
    "3, 10, 5, false"     // count less than expected
  })
  void checkAndCompleteJob_shouldHandleDifferentCounts(
    long jobExecutionId, long expectedCount, long processedCount, boolean shouldComplete) throws InterruptedException {
    // given
    var timeoutMs = shouldComplete ? 1000 : 100;

    // when
    if (shouldComplete) {
      new Thread(() -> {
        try {
          Thread.sleep(100);
          service.checkAndCompleteJob(jobExecutionId, processedCount);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }).start();
    } else {
      service.checkAndCompleteJob(jobExecutionId, processedCount);
    }

    var completed = service.awaitCompletion(jobExecutionId, expectedCount, timeoutMs, TimeUnit.MILLISECONDS);

    // then
    assertThat(completed).isEqualTo(shouldComplete);
  }

  @Test
  void completeJob_shouldCompleteAndCleanupJobImmediately() throws InterruptedException {
    // given
    var jobExecutionId = 4L;

    // when
    new Thread(() -> {
      try {
        Thread.sleep(100);
        service.completeJob(jobExecutionId);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }).start();

    var completed = service.awaitCompletion(jobExecutionId, 100L, 1, TimeUnit.SECONDS);

    // then
    assertThat(completed).isTrue();
  }
}

