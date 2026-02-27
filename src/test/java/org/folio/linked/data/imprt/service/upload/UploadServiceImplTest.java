package org.folio.linked.data.imprt.service.upload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
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
  void upload_shouldUseOriginalFileName() throws Exception {
    var fileName = "original.rdf";
    doReturn(false).when(multipartFile).isEmpty();
    doReturn(fileName).when(multipartFile).getOriginalFilename();
    doReturn(new ByteArrayInputStream("test".getBytes())).when(multipartFile).getInputStream();

    var result = uploadService.upload(multipartFile);

    assertThat(result).isEqualTo(fileName);
    verify(s3Service).upload(eq(fileName), any());
  }

  @Test
  void upload_shouldFailForEmptyFile() {
    doReturn(true).when(multipartFile).isEmpty();

    assertThatThrownBy(() -> uploadService.upload(multipartFile))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("File must not be empty");
  }

  @Test
  void upload_shouldFailForMissingFileName() {
    doReturn(false).when(multipartFile).isEmpty();
    doReturn("").when(multipartFile).getOriginalFilename();

    assertThatThrownBy(() -> uploadService.upload(multipartFile))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("File name must not be empty");
  }

  @Test
  void upload_shouldFailForNullOriginalFileName() {
    doReturn(false).when(multipartFile).isEmpty();
    doReturn(null).when(multipartFile).getOriginalFilename();

    assertThatThrownBy(() -> uploadService.upload(multipartFile))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessage("File name must not be empty");
  }

  @Test
  void upload_shouldSanitizePathTraversalOriginalFileName() throws Exception {
    doReturn(false).when(multipartFile).isEmpty();
    doReturn("../another-tenant/sample-upload.rdf").when(multipartFile).getOriginalFilename();
    doReturn(new ByteArrayInputStream("test".getBytes())).when(multipartFile).getInputStream();

    var result = uploadService.upload(multipartFile);

    assertThat(result).isEqualTo("another-tenant_sample-upload.rdf");
    verify(s3Service).upload(eq("another-tenant_sample-upload.rdf"), any());
  }

  @Test
  void upload_shouldSanitizeNestedPathOriginalFileName() throws Exception {
    doReturn(false).when(multipartFile).isEmpty();
    doReturn("nested/file.rdf").when(multipartFile).getOriginalFilename();
    doReturn(new ByteArrayInputStream("test".getBytes())).when(multipartFile).getInputStream();

    var result = uploadService.upload(multipartFile);

    assertThat(result).isEqualTo("nested_file.rdf");
    verify(s3Service).upload(eq("nested_file.rdf"), any());
  }

  @Test
  void upload_shouldWrapIoException() throws Exception {
    doReturn(false).when(multipartFile).isEmpty();
    doReturn("file.rdf").when(multipartFile).getOriginalFilename();
    doThrow(new IOException("boom")).when(multipartFile).getInputStream();

    assertThatThrownBy(() -> uploadService.upload(multipartFile))
      .isInstanceOf(UncheckedIOException.class)
      .hasMessageContaining("Failed to upload file to S3");
  }
}
