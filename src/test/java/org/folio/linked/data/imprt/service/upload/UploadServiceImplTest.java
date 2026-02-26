package org.folio.linked.data.imprt.service.upload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.folio.linked.data.imprt.service.s3.S3Service;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.multipart.MultipartFile;

@UnitTest
@ExtendWith(MockitoExtension.class)
class UploadServiceImplTest {

  @InjectMocks
  private UploadServiceImpl uploadService;
  @Mock
  private S3Service s3Service;
  @Mock
  private MultipartFile multipartFile;

  @Test
  void upload_shouldUseProvidedFileName() throws Exception {
    var providedFileName = "override.rdf";
    doReturn(false).when(multipartFile).isEmpty();
    doReturn(new ByteArrayInputStream("test".getBytes())).when(multipartFile).getInputStream();

    var result = uploadService.upload(multipartFile, providedFileName);

    assertThat(result).isEqualTo(providedFileName);
    verify(s3Service).upload(eq(providedFileName), any());
  }

  @Test
  void upload_shouldUseOriginalFileName_whenFileNameNotProvided() throws Exception {
    var originalFileName = "original.rdf";
    doReturn(false).when(multipartFile).isEmpty();
    doReturn(originalFileName).when(multipartFile).getOriginalFilename();
    doReturn(new ByteArrayInputStream("test".getBytes())).when(multipartFile).getInputStream();

    var result = uploadService.upload(multipartFile, null);

    assertThat(result).isEqualTo(originalFileName);
    verify(s3Service).upload(eq(originalFileName), any());
  }

  @Test
  void upload_shouldFailForEmptyFile() {
    doReturn(true).when(multipartFile).isEmpty();

    assertThatThrownBy(() -> uploadService.upload(multipartFile, null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("File must not be empty");
  }

  @Test
  void upload_shouldFailForMissingFileName() {
    doReturn(false).when(multipartFile).isEmpty();
    doReturn("").when(multipartFile).getOriginalFilename();

    assertThatThrownBy(() -> uploadService.upload(multipartFile, null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("File name must not be empty");
  }

  @Test
  void upload_shouldWrapIoException() throws Exception {
    doReturn(false).when(multipartFile).isEmpty();
    doReturn("file.rdf").when(multipartFile).getOriginalFilename();
    org.mockito.Mockito.doThrow(new IOException("boom")).when(multipartFile).getInputStream();

    assertThatThrownBy(() -> uploadService.upload(multipartFile, null))
      .isInstanceOf(java.io.UncheckedIOException.class)
      .hasMessageContaining("Failed to upload file to S3");
  }
}
