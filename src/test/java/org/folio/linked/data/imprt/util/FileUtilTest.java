package org.folio.linked.data.imprt.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class FileUtilTest {

  @Test
  void extractFileName_shouldReturnFileName_forS3Url() {
    // given
    var url = "s3://bucket/path/to/file.rdf";

    // when
    var name = FileUtil.extractFileName(url);

    // then
    assertThat(name).isEqualTo("file.rdf");
  }

  @Test
  void extractFileName_shouldReturnFileName_forPlainName() {
    // given
    var url = "file.rdf";

    // when
    var name = FileUtil.extractFileName(url);

    // then
    assertThat(name).isEqualTo("file.rdf");
  }

  @Test
  void extractFileName_shouldReturnLastSegment_forMultiSlashPath() {
    // given
    var url = "/var/tmp/data/file.txt";

    // when
    var name = FileUtil.extractFileName(url);

    // then
    assertThat(name).isEqualTo("file.txt");
  }

  @Test
  void extractFileName_shouldReturnEmpty_forPathEndingWithSlash() {
    // given
    var url = "s3://bucket/path/";

    // when
    var name = FileUtil.extractFileName(url);

    // then
    assertThat(name).isEmpty();
  }
}

