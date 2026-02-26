package org.folio.linked.data.imprt.controller;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.folio.linked.data.imprt.controller.advice.ApiExceptionHandler;
import org.folio.linked.data.imprt.service.upload.UploadService;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@UnitTest
@ExtendWith(MockitoExtension.class)
class UploadControllerTest {

  private MockMvc mockMvc;

  @InjectMocks
  private UploadController uploadController;
  @Mock
  private UploadService uploadService;

  @BeforeEach
  void setUp() {
    mockMvc = MockMvcBuilders.standaloneSetup(uploadController)
      .setControllerAdvice(new ApiExceptionHandler())
      .build();
  }

  @Test
  void uploadFile_shouldReturnUploadedFileName() throws Exception {
    var fileName = "my-file.rdf";
    var multipartFile = new MockMultipartFile("file", fileName, MediaType.TEXT_PLAIN_VALUE, "test".getBytes());
    doReturn(fileName).when(uploadService).upload(any(), eq(null));

    mockMvc.perform(multipart("/linked-data-import/files").file(multipartFile))
      .andExpect(status().isOk())
      .andExpect(content().string(fileName));

    verify(uploadService).upload(any(), eq(null));
  }

  @Test
  void uploadFile_shouldUseProvidedFileName() throws Exception {
    var providedFileName = "override.rdf";
    var multipartFile = new MockMultipartFile("file", "ignored.rdf", MediaType.TEXT_PLAIN_VALUE, "test".getBytes());
    doReturn(providedFileName).when(uploadService).upload(any(), eq(providedFileName));

    mockMvc.perform(multipart("/linked-data-import/files")
        .file(multipartFile)
        .param("fileName", providedFileName))
      .andExpect(status().isOk())
      .andExpect(content().string(providedFileName));

    verify(uploadService).upload(any(), eq(providedFileName));
  }

  @Test
  void uploadFile_shouldFailForEmptyFile() {
    var multipartFile = new MockMultipartFile("file", "empty.rdf", MediaType.TEXT_PLAIN_VALUE, new byte[0]);
    doThrow(new IllegalArgumentException("File must not be empty")).when(uploadService).upload(any(), eq(null));

    assertThatCode(() -> mockMvc.perform(multipart("/linked-data-import/files")
        .file(multipartFile))
      .andExpect(status().isBadRequest()))
      .doesNotThrowAnyException();
  }

  @Test
  void uploadFile_shouldFailForPathTraversalFileName() {
    var multipartFile = new MockMultipartFile("file", "safe.rdf", MediaType.TEXT_PLAIN_VALUE, "test".getBytes());
    doThrow(new IllegalArgumentException("File name must not contain path separators"))
      .when(uploadService).upload(any(), eq("../evil.rdf"));

    assertThatCode(() -> mockMvc.perform(multipart("/linked-data-import/files")
        .file(multipartFile)
        .param("fileName", "../evil.rdf"))
      .andExpect(status().isBadRequest()))
      .doesNotThrowAnyException();
  }
}
