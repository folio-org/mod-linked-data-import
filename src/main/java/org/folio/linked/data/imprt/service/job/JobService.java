package org.folio.linked.data.imprt.service.job;

import org.folio.linked.data.imprt.domain.dto.JobInfo;
import org.springframework.core.io.Resource;

public interface JobService {

  JobInfo getJobInfo(Long jobExecutionId);

  Long getMappedCount(Long jobExecutionId);

  long getSavedCount(Long jobExecutionId);

  Resource generateFailedLinesCsv(Long jobExecutionId);

  void cancelJob(Long jobExecutionId);
}

