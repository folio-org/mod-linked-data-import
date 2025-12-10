package org.folio.linked.data.imprt.service.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
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
    long jobExecutionId, long expectedCount, long processedCount, boolean shouldComplete) throws Exception {
    // given
    var timeoutMs = shouldComplete ? 1000 : 100;

    // when - start awaitCompletion in separate thread
    var awaitFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return service.awaitCompletion(jobExecutionId, expectedCount, timeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    });

    // Give time for latch registration
    await().pollDelay(Duration.ofMillis(10))
      .atMost(Duration.ofMillis(100))
      .until(() -> true);

    service.checkAndCompleteJob(jobExecutionId, processedCount);
    var completed = awaitFuture.get();

    // then
    assertThat(completed).isEqualTo(shouldComplete);
  }

  @Test
  void completeJob_shouldCompleteAndCleanupJobImmediately() throws Exception {
    // given
    var jobExecutionId = 4L;

    // when - start awaitCompletion in separate thread
    var awaitFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return service.awaitCompletion(jobExecutionId, 100L, 1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    });

    // Give time for latch registration
    await().pollDelay(Duration.ofMillis(10))
      .atMost(Duration.ofMillis(100))
      .until(() -> true);

    service.completeJob(jobExecutionId);
    var completed = awaitFuture.get();

    // then
    assertThat(completed).isTrue();
  }
}

