package org.folio.linked.data.imprt.repo;

import org.folio.linked.data.imprt.model.entity.BatchStepExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface BatchStepExecutionRepo extends JpaRepository<BatchStepExecution, Long> {

  @Query(value = """
    SELECT SUM(s.read_count) FROM batch_step_execution s
    JOIN batch_job_execution e ON s.job_execution_id = e.job_execution_id
    WHERE e.job_instance_id = :jobInstanceId
    """, nativeQuery = true)
  Long getTotalReadCountByJobInstanceId(Long jobInstanceId);
}

