package org.folio.linked.data.imprt.controller;

import lombok.RequiredArgsConstructor;
import org.folio.linked.data.imprt.domain.dto.DefaultWorkType;
import org.folio.linked.data.imprt.rest.resource.ImportStartApi;
import org.folio.linked.data.imprt.service.imprt.ImportJobService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ImportController implements ImportStartApi {
  private final ImportJobService importJobService;

  @Override
  public ResponseEntity<String> startImport(String fileUrl, String contentType, DefaultWorkType defaultWorkType) {
    var jobId = importJobService.start(fileUrl, contentType, defaultWorkType);
    return ResponseEntity.ok(String.valueOf(jobId));
  }
}
