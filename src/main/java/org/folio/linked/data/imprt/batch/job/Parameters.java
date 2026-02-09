package org.folio.linked.data.imprt.batch.job;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Parameters {
  public static final String FILE_NAME = "fileName";
  public static final String CONTENT_TYPE = "contentType";
  public static final String DEFAULT_WORK_TYPE = "defaultWorkType";
  public static final String STARTED_BY = "startedBy";
  public static final String TMP_DIR = System.getProperty("java.io.tmpdir");
}
