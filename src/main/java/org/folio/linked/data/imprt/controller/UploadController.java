package org.folio.linked.data.imprt.controller;

import lombok.RequiredArgsConstructor;
import org.folio.linked.data.imprt.rest.resource.UploadApi;
import org.folio.linked.data.imprt.service.upload.UploadService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequiredArgsConstructor
public class UploadController implements UploadApi {

  private final UploadService uploadService;

  @Override
  public ResponseEntity<String> uploadFile(MultipartFile file) {
    return ResponseEntity.ok(uploadService.upload(file));
  }
}
