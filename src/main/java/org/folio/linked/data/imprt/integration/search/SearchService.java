package org.folio.linked.data.imprt.integration.search;

import java.util.Optional;

public interface SearchService {

  Optional<String> searchForInventoryId(String lccn);
}
