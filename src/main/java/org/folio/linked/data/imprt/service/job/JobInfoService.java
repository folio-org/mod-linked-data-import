package org.folio.linked.data.imprt.service.job;

import org.folio.linked.data.imprt.domain.dto.JobInfo;
import org.springframework.core.io.Resource;

public interface JobInfoService {

  JobInfo getJobInfo(Long jobId);

  Resource generateFailedLinesCsv(Long jobId);
}

