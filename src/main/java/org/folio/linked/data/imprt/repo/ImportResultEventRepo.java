package org.folio.linked.data.imprt.repo;

import org.folio.linked.data.imprt.model.entity.ImportResultEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ImportResultEventRepo extends JpaRepository<ImportResultEvent, Long> {
}

