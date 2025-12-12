package org.folio.linked.data.imprt.service.file;

import static org.folio.linked.data.imprt.batch.job.Parameters.TMP_DIR;
import static org.folio.linked.data.imprt.util.FileUtil.extractFileName;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class FileServiceImpl implements FileService {

  private static final String OUT_OF_BOUNDS = "Line number %s is out of bounds for file %s";
  private static final String ERROR_READING_FILE = "Error reading file: %s";
  private static final String FILE_NOT_FOUND = "File not found: %s";

  @Override
  public String readLineFromFile(String fileUrl, Long lineNumber) {
    var fileName = extractFileName(fileUrl);
    var file = new File(TMP_DIR, fileName);

    if (!file.exists()) {
      var notFound = FILE_NOT_FOUND.formatted(fileName);
      log.warn(notFound);
      return notFound;
    }

    try (var reader = new BufferedReader(new FileReader(file))) {
      var linesToSkip = lineNumber - 1;
      var skipped = reader.lines().skip(linesToSkip).findFirst();

      if (skipped.isEmpty()) {
        var outOfBounds = OUT_OF_BOUNDS.formatted(lineNumber, fileName);
        log.error(outOfBounds);
        return outOfBounds;
      }

      return skipped.get();
    } catch (IOException e) {
      var errorReadingFile = ERROR_READING_FILE.formatted(fileName);
      log.error(errorReadingFile, e);
      return errorReadingFile;
    }
  }
}

