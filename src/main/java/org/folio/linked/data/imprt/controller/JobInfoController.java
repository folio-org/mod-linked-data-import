package org.folio.linked.data.imprt.controller;

import lombok.RequiredArgsConstructor;
import org.folio.linked.data.imprt.domain.dto.JobInfo;
import org.folio.linked.data.imprt.rest.resource.ImportJobApi;
import org.folio.linked.data.imprt.service.job.JobInfoService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class JobInfoController implements ImportJobApi {

  private final JobInfoService jobInfoService;

  @Override
  public ResponseEntity<JobInfo> getJobInfo(Long jobId) {
    var jobInfo = jobInfoService.getJobInfo(jobId);
    return ResponseEntity.ok(jobInfo);
  }
}

