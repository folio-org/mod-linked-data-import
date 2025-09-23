package org.folio.linked.data.imprt.controller;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.verify;
import static org.springframework.http.HttpStatus.ACCEPTED;

import org.folio.linked.data.imprt.service.imprt.ImportJobService;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;

@UnitTest
@ExtendWith(MockitoExtension.class)
class ImportControllerTest {

  @InjectMocks
  private ImportController importController;
  @Mock
  private ImportJobService importJobService;

  @Test
  void importStart_shouldReturnAcceptedResponse() {
    // given
    var fileUrl = "http://example.com/file";
    var contentType = "application/json";

    // when
    var result = importController.startImport(fileUrl, contentType);

    // then
    verify(importJobService).start(fileUrl, contentType);
    assertThat(result).isEqualTo(new ResponseEntity<>(ACCEPTED));
  }
}
