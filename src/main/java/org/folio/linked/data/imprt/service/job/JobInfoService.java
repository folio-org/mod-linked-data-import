package org.folio.linked.data.imprt.service.job;

import org.folio.linked.data.imprt.domain.dto.JobInfo;

public interface JobInfoService {

  JobInfo getJobInfo(Long jobId);
}

