package org.folio.linked.data.imprt.integration.srs;

import java.util.Optional;
import org.folio.ld.dictionary.model.Resource;

public interface SrsService {

  Optional<Resource> fetchAuthorityFromSrs(String inventoryId);
}
