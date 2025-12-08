package org.folio.linked.data.imprt.repo;

import java.util.Optional;
import org.folio.linked.data.imprt.model.entity.BatchJobExecutionParams;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface BatchJobExecutionParamsRepo extends
    JpaRepository<BatchJobExecutionParams, BatchJobExecutionParams.BatchJobExecutionParamsId> {

  @Query(value = """
    SELECT p.* FROM batch_job_execution_params p
    JOIN batch_job_execution e ON p.job_execution_id = e.job_execution_id
    WHERE e.job_instance_id = :jobInstanceId
    AND p.parameter_name = :parameterName
    """, nativeQuery = true)
  Optional<BatchJobExecutionParams> findByJobInstanceIdAndParameterName(Long jobInstanceId, String parameterName);
}

