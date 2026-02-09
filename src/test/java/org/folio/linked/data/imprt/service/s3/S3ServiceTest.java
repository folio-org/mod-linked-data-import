package org.folio.linked.data.imprt.service.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.EMPTY_LIST;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.doReturn;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.util.List;
import org.folio.s3.client.FolioS3Client;
import org.folio.spring.FolioExecutionContext;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class S3ServiceTest {

  @InjectMocks
  private S3ServiceImpl s3Service;
  @Mock
  private FolioS3Client folioS3Client;
  @Mock
  private FolioExecutionContext folioExecutionContext;

  @Test
  void exists_shouldReturnTrue_ifFolioS3ClientListIsNotEmpty() {
    // given
    var fileUrl = "s3://bucket/key";
    var tenantId = "test-tenant";
    doReturn(tenantId).when(folioExecutionContext).getTenantId();
    doReturn(List.of("someObject")).when(folioS3Client).list(tenantId + "/" + fileUrl);

    // when
    boolean result = s3Service.exists(fileUrl);

    // then
    assertThat(result).isTrue();
  }

  @Test
  void exists_shouldReturnFalse_ifFolioS3ClientListIsEmpty() {
    // given
    var fileUrl = "s3://bucket/key";
    var tenantId = "test-tenant";
    doReturn(tenantId).when(folioExecutionContext).getTenantId();
    doReturn(EMPTY_LIST).when(folioS3Client).list(tenantId + "/" + fileUrl);

    // when
    boolean result = s3Service.exists(fileUrl);

    // then
    assertThat(result).isFalse();
  }

  @Test
  void download_shouldCopyFileProvidedByFolioS3ClientToDestinationPath() throws Exception {
    // given
    var fileName = "file.txt";
    var tenantId = "test-tenant";
    var destinationPath = System.getProperty("java.io.tmpdir");
    var fileContent = "some data in a file";
    var is = new ByteArrayInputStream(fileContent.getBytes(UTF_8));
    doReturn(tenantId).when(folioExecutionContext).getTenantId();
    doReturn(is).when(folioS3Client).read(tenantId + "/" + fileName);

    // when
    s3Service.download(fileName, destinationPath);

    // then
    var targetFile = new java.io.File(destinationPath, fileName);
    assertThat(targetFile).exists();
    var downloadedContent = Files.readString(targetFile.toPath(), UTF_8);
    assertThat(downloadedContent).isEqualTo(fileContent);

    // cleanup
    Files.delete(targetFile.toPath());
  }

}
