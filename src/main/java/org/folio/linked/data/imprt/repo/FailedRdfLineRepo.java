package org.folio.linked.data.imprt.repo;

import java.util.stream.Stream;
import org.folio.linked.data.imprt.model.entity.FailedRdfLine;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface FailedRdfLineRepo extends JpaRepository<FailedRdfLine, Long> {

  @Query("SELECT COUNT(f) FROM FailedRdfLine f WHERE f.jobExecutionId = :executionId AND f.importResultEvent IS NULL")
  Long countFailedLinesWithoutImportResultEvent(Long executionId);

  Stream<FailedRdfLine> findAllByJobExecutionIdOrderByLineNumber(Long jobExecutionId);
}
