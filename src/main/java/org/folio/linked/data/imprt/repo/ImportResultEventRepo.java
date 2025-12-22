package org.folio.linked.data.imprt.repo;

import java.util.List;
import org.folio.linked.data.imprt.model.entity.ImportResultEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface ImportResultEventRepo extends JpaRepository<ImportResultEvent, Long> {

  @Query("SELECT COALESCE(SUM(e.resourcesCount), 0) FROM ImportResultEvent e WHERE e.jobExecutionId = :jobExecutionId")
  Long getTotalResourcesCountByJobExecutionId(Long jobExecutionId);

  List<ImportResultEvent> findAllByJobExecutionId(Long jobExecutionId);
}

