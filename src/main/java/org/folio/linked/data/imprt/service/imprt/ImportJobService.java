package org.folio.linked.data.imprt.service.imprt;

import org.folio.linked.data.imprt.domain.dto.DefaultWorkType;

public interface ImportJobService {

  Long start(String fileUrl, String contentType, DefaultWorkType defaultWorkType);

}
