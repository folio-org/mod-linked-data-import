package org.folio.linked.data.imprt.service.s3;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.s3.client.FolioS3Client;
import org.folio.spring.FolioExecutionContext;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class S3ServiceImpl implements S3Service {
  private final FolioS3Client folioS3Client;
  private final FolioExecutionContext folioExecutionContext;

  @Override
  public boolean exists(String fileName) {
    return !folioS3Client.list(filePathWithTenantFolder(fileName)).isEmpty();
  }

  @Override
  public void download(String fileName, String destinationPath) throws IOException {
    try (var is = folioS3Client.read(filePathWithTenantFolder(fileName))) {
      var targetFile = new File(destinationPath, fileName);
      Files.copy(is, targetFile.toPath(), REPLACE_EXISTING);
    }
    log.info("File {} downloaded successfully to {}", fileName, destinationPath + "/" + fileName);
  }

  private String filePathWithTenantFolder(String fileName) {
    return folioExecutionContext.getTenantId() + "/" + fileName;
  }

}
