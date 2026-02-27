package org.folio.linked.data.imprt.service.upload;

import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.RequiredArgsConstructor;
import org.folio.linked.data.imprt.service.s3.S3Service;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

@Service
@RequiredArgsConstructor
public class UploadServiceImpl implements UploadService {

  private final S3Service s3Service;

  @Override
  public String upload(MultipartFile file) {
    if (file.isEmpty()) {
      throw new IllegalArgumentException("File must not be empty");
    }

    var resolvedFileName = resolveFileName(file);

    try (var inputStream = file.getInputStream()) {
      s3Service.upload(resolvedFileName, inputStream);
      return resolvedFileName;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to upload file to S3", e);
    }
  }

  private String resolveFileName(MultipartFile file) {
    var originalFileName = file.getOriginalFilename();
    if (!StringUtils.hasText(originalFileName)) {
      throw new IllegalArgumentException("File name must not be empty");
    }
    if (originalFileName.contains("/") || originalFileName.contains("\\")) {
      throw new IllegalArgumentException("File name must not contain path separators");
    }
    return originalFileName;
  }
}
