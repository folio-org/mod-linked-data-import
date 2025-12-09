package org.folio.linked.data.imprt.repo;

import java.util.List;
import org.folio.linked.data.imprt.model.entity.FailedRdfLine;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface FailedRdfLineRepo extends JpaRepository<FailedRdfLine, Long> {

  @Query("SELECT COUNT(f) FROM FailedRdfLine f WHERE f.jobInstanceId = :jobInstanceId AND f.importResultEvent IS NULL")
  Long countFailedLinesWithoutImportResultEvent(Long jobInstanceId);

  List<FailedRdfLine> findAllByJobInstanceIdOrderByLineNumber(Long jobInstanceId);
}
