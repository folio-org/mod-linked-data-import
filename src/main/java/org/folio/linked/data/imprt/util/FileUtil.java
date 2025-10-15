package org.folio.linked.data.imprt.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class FileUtil {

  public static String extractFileName(String fileUrl) {
    return fileUrl.substring(fileUrl.lastIndexOf("/") + 1);
  }
}
