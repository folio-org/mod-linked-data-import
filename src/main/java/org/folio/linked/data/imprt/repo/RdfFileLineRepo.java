package org.folio.linked.data.imprt.repo;

import java.util.Optional;
import org.folio.linked.data.imprt.model.entity.RdfFileLine;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RdfFileLineRepo extends JpaRepository<RdfFileLine, Long> {

  Optional<RdfFileLine> findByJobExecutionIdAndLineNumber(Long jobExecutionId, Long lineNumber);

  long deleteByJobExecutionId(Long jobExecutionId);
}

