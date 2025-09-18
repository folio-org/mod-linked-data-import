package org.folio.linked.data.imprt.controller;

import static org.springframework.http.HttpStatus.ACCEPTED;

import lombok.RequiredArgsConstructor;
import org.folio.linked.data.imprt.service.ImportJobService;
import org.folio.linked.data.rest.resource.ImportStartApi;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
@RequiredArgsConstructor
public class ImportController implements ImportStartApi {
  private final ImportJobService importJobService;

  @Override
  public ResponseEntity<Void> startImport(String fileUrl, String contentType) {
    importJobService.start(fileUrl, contentType);
    return new ResponseEntity<>(ACCEPTED);
  }
}
