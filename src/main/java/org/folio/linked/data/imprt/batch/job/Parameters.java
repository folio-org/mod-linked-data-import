package org.folio.linked.data.imprt.batch.job;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Parameters {
  public static final String FILE_URL = "fileUrl";
  public static final String CONTENT_TYPE = "contentType";
  public static final String DATE_START = "dateStart";
  public static final String TMP_DIR = System.getProperty("java.io.tmpdir");
}
