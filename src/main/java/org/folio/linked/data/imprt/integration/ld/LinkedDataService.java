package org.folio.linked.data.imprt.integration.ld;

import java.util.Set;
import org.folio.ld.dictionary.model.Resource;

public interface LinkedDataService {

  Set<Resource> searchResources(Set<String> inventoryIds);
}
