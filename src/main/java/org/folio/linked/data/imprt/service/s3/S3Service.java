package org.folio.linked.data.imprt.service.s3;

import java.io.IOException;
import java.io.InputStream;

public interface S3Service {

  boolean exists(String fileName);

  void upload(String fileName, InputStream inputStream) throws IOException;

  void download(String fileName, String destinationPath) throws IOException;

}
