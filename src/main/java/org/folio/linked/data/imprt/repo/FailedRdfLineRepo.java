package org.folio.linked.data.imprt.repo;

import org.folio.linked.data.imprt.model.FailedRdfLine;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FailedRdfLineRepo extends JpaRepository<FailedRdfLine, Long> {
}
