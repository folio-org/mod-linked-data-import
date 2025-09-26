package org.folio.linked.data.imprt.service.s3;

import java.io.IOException;

public interface S3Service {

  boolean exists(String fileUrl);

  void download(String fileUrl, String destinationPath) throws IOException;

}
