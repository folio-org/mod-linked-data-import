package org.folio.linked.data.imprt.controller;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.doReturn;

import org.folio.linked.data.imprt.domain.dto.DefaultWorkType;
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
  void importStart_shouldReturnOkResponse() {
    var fileUrl = "http://example.com/file";
    var contentType = "application/json";
    var defaultWorkType = DefaultWorkType.MONOGRAPH;
    var jobExecutionId = 123L;
    doReturn(jobExecutionId).when(importJobService).start(fileUrl, contentType, defaultWorkType);

    var result = importController.startImport(fileUrl, contentType, defaultWorkType);

    assertThat(result).isEqualTo(ResponseEntity.ok(String.valueOf(jobExecutionId)));
  }
}
