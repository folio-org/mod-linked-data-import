package org.folio.linked.data.imprt.repo;

import java.util.Optional;
import org.folio.linked.data.imprt.model.entity.BatchStepExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface BatchStepExecutionRepo extends JpaRepository<BatchStepExecution, Long> {

  @Query(value = """
    SELECT COALESCE(SUM(s.read_count), 0) FROM batch_step_execution s
    WHERE s.job_execution_id = :jobExecutionId
    """, nativeQuery = true)
  Long getTotalReadCountByJobExecutionId(Long jobExecutionId);

  @Query(value = """
    SELECT COALESCE(SUM(s.write_count), 0) FROM batch_step_execution s
    WHERE s.job_execution_id = :jobExecutionId AND s.step_name = 'mappingStep'
    """, nativeQuery = true)
  Long getMappedCountByJobExecutionId(Long jobExecutionId);

  @Query(value = """
    SELECT s.step_name FROM batch_step_execution s
    WHERE s.job_execution_id = :jobExecutionId
    ORDER BY
      CASE
        WHEN s.status = 'STARTED' THEN 1
        WHEN s.status = 'STARTING' THEN 2
        ELSE 3
      END,
      s.start_time DESC
    LIMIT 1
    """, nativeQuery = true)
  Optional<String> findLastStepNameByJobExecutionId(Long jobExecutionId);
}

