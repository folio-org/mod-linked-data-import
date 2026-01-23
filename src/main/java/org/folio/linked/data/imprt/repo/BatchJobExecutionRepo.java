package org.folio.linked.data.imprt.repo;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.folio.linked.data.imprt.model.entity.BatchJobExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BatchJobExecutionRepo extends JpaRepository<BatchJobExecution, Long> {

  Optional<BatchJobExecution> findByJobExecutionId(Long jobExecutionId);

  List<BatchJobExecution> findByStartTimeBefore(LocalDateTime cutoffDate);
}

