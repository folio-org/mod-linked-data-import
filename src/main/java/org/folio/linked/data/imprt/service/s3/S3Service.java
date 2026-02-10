package org.folio.linked.data.imprt.service.s3;

import java.io.IOException;

public interface S3Service {

  boolean exists(String fileName);

  void download(String fileName, String destinationPath) throws IOException;

}
