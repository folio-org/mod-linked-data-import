package org.folio.linked.data.imprt.e2e;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.linked.data.imprt.rest.resource.UploadApi.PATH_UPLOAD_FILE;
import static org.folio.linked.data.imprt.test.TestUtil.TENANT_ID;
import static org.folio.linked.data.imprt.test.TestUtil.defaultHeaders;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.folio.linked.data.imprt.test.IntegrationTest;
import org.folio.s3.client.FolioS3Client;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

@IntegrationTest
class UploadIT {

  @Autowired
  private MockMvc mockMvc;
  @Autowired
  private FolioS3Client s3Client;

  @Test
  void uploadFile_shouldStoreFileInTenantFolder() throws Exception {
    // given
    var fileName = "upload-it-file.rdf";
    var fileContent = "line 1\nline 2\nline 3";
    var file = new MockMultipartFile("file", fileName, "text/plain", fileContent.getBytes(UTF_8));

    // when
    var response = mockMvc.perform(multipart(PATH_UPLOAD_FILE)
        .file(file)
        .headers(defaultHeaders()))
      .andExpect(status().isOk())
      .andReturn()
      .getResponse();

    // then
    assertThat(response.getContentAsString()).isEqualTo(fileName);
    try (var s3File = s3Client.read(TENANT_ID + "/" + fileName)) {
      assertThat(new String(s3File.readAllBytes(), UTF_8)).isEqualTo(fileContent);
    }
  }

  @Test
  void uploadFile_shouldUseProvidedFileName() throws Exception {
    // given
    var originalFileName = "original-name.rdf";
    var targetFileName = "target-name.rdf";
    var fileContent = "rdf content";
    var file = new MockMultipartFile("file", originalFileName, "text/plain", fileContent.getBytes(UTF_8));

    // when
    var response = mockMvc.perform(multipart(PATH_UPLOAD_FILE)
        .file(file)
        .param("fileName", targetFileName)
        .headers(defaultHeaders()))
      .andExpect(status().isOk())
      .andReturn()
      .getResponse();

    // then
    assertThat(response.getContentAsString()).isEqualTo(targetFileName);
    try (var s3File = s3Client.read(TENANT_ID + "/" + targetFileName)) {
      assertThat(new String(s3File.readAllBytes(), UTF_8)).isEqualTo(fileContent);
    }
  }
}
