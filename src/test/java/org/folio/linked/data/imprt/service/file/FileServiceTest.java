package org.folio.linked.data.imprt.service.file;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@UnitTest
class FileServiceTest {

  private static final String TEST_FILE_NAME = "test-file.rdf";
  private final FileService fileService = new FileServiceImpl();
  private File testFile;

  @BeforeEach
  void setUp() throws IOException {
    testFile = new File(TMP_DIR, TEST_FILE_NAME);
    Files.writeString(testFile.toPath(), """
      Line 1
      Line 2
      Line 3
      Line 4
      Line 5

      """, UTF_8);
  }

  @AfterEach
  void tearDown() {
    if (testFile != null && testFile.exists()) {
      var deleted = testFile.delete();
      if (!deleted) {
        testFile.deleteOnExit();
      }
    }
  }

  @ParameterizedTest
  @MethodSource("provideTestCases")
  void readLineFromFile_shouldHandleVariousCases(String fileUrl, long lineNumber, String expectedResult) {
    // given
    // parameters provided by provideTestCases

    // when
    var result = fileService.readLineFromFile(fileUrl, lineNumber);

    // then
    assertThat(result).isEqualTo(expectedResult);
  }

  private static Stream<Arguments> provideTestCases() {
    return Stream.of(
      Arguments.of("s3://bucket/" + TEST_FILE_NAME, 1L, "Line 1"),
      Arguments.of("s3://bucket/" + TEST_FILE_NAME, 3L, "Line 3"),
      Arguments.of("s3://bucket/" + TEST_FILE_NAME, 5L, "Line 5"),
      Arguments.of("s3://bucket/" + TEST_FILE_NAME, 6L, ""),
      Arguments.of("s3://bucket/" + TEST_FILE_NAME, 100L,
        "Line number 100 is out of bounds for file " + TEST_FILE_NAME),
      Arguments.of("s3://bucket/non-existent-file.txt", 1L, "File not found: non-existent-file.txt")
    );
  }
}

