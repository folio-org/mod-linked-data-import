package org.folio.linked.data.imprt.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class FileUtilTest {

  @ParameterizedTest
  @MethodSource("fileNameCases")
  void extractFileName_shouldReturnExpected(String input, String expected) {
    // when
    var name = FileUtil.extractFileName(input);
    // then
    assertThat(name).isEqualTo(expected);
  }

  static Stream<Arguments> fileNameCases() {
    return Stream.of(
      Arguments.of("s3://bucket/path/to/file.rdf", "file.rdf"),
      Arguments.of("file.rdf", "file.rdf"),
      Arguments.of("/var/tmp/data/file.txt", "file.txt")
    );
  }

}

