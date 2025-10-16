package org.folio.linked.data.imprt.service.s3;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.folio.linked.data.imprt.util.FileUtil.extractFileName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.s3.client.FolioS3Client;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class S3ServiceImpl implements S3Service {
  private final FolioS3Client folioS3Client;

  @Override
  public boolean exists(String fileUrl) {
    return !folioS3Client.list(fileUrl).isEmpty();
  }

  @Override
  public void download(String fileUrl, String destinationPath) throws IOException {
    var is = folioS3Client.read(fileUrl);
    var fileName = extractFileName(fileUrl);
    var targetFile = new File(destinationPath, fileName);
    Files.copy(is, targetFile.toPath(), REPLACE_EXISTING);
    is.close();
    log.info("File {} downloaded successfully to {}", fileUrl, destinationPath + "/" + fileName);
  }

}
