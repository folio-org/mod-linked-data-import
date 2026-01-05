package org.folio.linked.data.imprt.controller;

import lombok.RequiredArgsConstructor;
import org.folio.linked.data.imprt.domain.dto.JobInfo;
import org.folio.linked.data.imprt.rest.resource.ImportJobApi;
import org.folio.linked.data.imprt.service.job.JobService;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class JobController implements ImportJobApi {

  private final JobService jobService;

  @Override
  public ResponseEntity<JobInfo> getJobInfo(Long jobExecutionId) {
    var jobInfo = jobService.getJobInfo(jobExecutionId);
    return ResponseEntity.ok(jobInfo);
  }

  @Override
  public ResponseEntity<Resource> getFailedLines(Long jobExecutionId) {
    return ResponseEntity.ok(jobService.generateFailedLinesCsv(jobExecutionId));
  }

  @Override
  public ResponseEntity<Void> cancelJob(Long jobExecutionId) {
    jobService.cancelJob(jobExecutionId);
    return ResponseEntity.ok().build();
  }
}

