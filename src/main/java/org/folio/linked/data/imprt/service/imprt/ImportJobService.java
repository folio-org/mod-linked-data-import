package org.folio.linked.data.imprt.service.imprt;

public interface ImportJobService {

  Long start(String fileUrl, String contentType);

}
