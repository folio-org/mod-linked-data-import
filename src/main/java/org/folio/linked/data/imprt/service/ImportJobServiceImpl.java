package org.folio.linked.data.imprt.service;

import static org.folio.linked.data.imprt.batch.job.Parameters.CONTENT_TYPE;
import static org.folio.linked.data.imprt.batch.job.Parameters.DATE_START;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ImportJobServiceImpl implements ImportJobService {

  private final Job rdfImportJob;
  private final JobLauncher jobLauncher;

  @Override
  public void start(String fileUrl, String contentType) {
    var jobParameters = new JobParametersBuilder()
      .addString(FILE_URL, fileUrl)
      .addString(CONTENT_TYPE, contentType)
      .addLong(DATE_START, System.currentTimeMillis())
      .toJobParameters();

    try {
      jobLauncher.run(rdfImportJob, jobParameters);
    } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
             | JobParametersInvalidException e) {
      throw new RuntimeException(e);
    }
  }

}
