package org.folio.linked.data.imprt.service.upload;

import org.springframework.web.multipart.MultipartFile;

public interface UploadService {

  String upload(MultipartFile file, String fileName);
}
