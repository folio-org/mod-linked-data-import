package org.folio.linked.data.imprt.service.imprt;

import static java.util.Objects.nonNull;
import static org.folio.linked.data.imprt.batch.job.Parameters.CONTENT_TYPE;
import static org.folio.linked.data.imprt.batch.job.Parameters.DATE_START;
import static org.folio.linked.data.imprt.batch.job.Parameters.DEFAULT_WORK_TYPE;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.springframework.util.ObjectUtils.isEmpty;

import lombok.RequiredArgsConstructor;
import org.folio.linked.data.imprt.domain.dto.DefaultWorkType;
import org.folio.linked.data.imprt.service.s3.S3Service;
import org.folio.spring.exception.NotFoundException;
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

  private static final String DEFAULT_CONTENT_TYPE = "application/ld+json";
  private final Job rdfImportJob;
  private final JobLauncher jobLauncher;
  private final S3Service s3Service;

  @Override
  public Long start(String fileUrl, String contentType, DefaultWorkType defaultWorkType) {
    checkFile(fileUrl);
    var jobParametersBuilder = new JobParametersBuilder()
      .addString(FILE_URL, fileUrl)
      .addString(CONTENT_TYPE, isEmpty(contentType) ? DEFAULT_CONTENT_TYPE : contentType)
      .addLong(DATE_START, System.currentTimeMillis());
    if (nonNull(defaultWorkType)) {
      jobParametersBuilder.addJobParameter(DEFAULT_WORK_TYPE, defaultWorkType, DefaultWorkType.class);
    }
    var jobParameters = jobParametersBuilder.toJobParameters();

    try {
      var jobExecution = jobLauncher.run(rdfImportJob, jobParameters);
      return jobExecution.getJobInstance().getInstanceId();
    } catch (JobExecutionAlreadyRunningException
             | JobRestartException
             | JobInstanceAlreadyCompleteException
             | JobParametersInvalidException e) {
      throw new IllegalArgumentException("Job launch exception", e);
    }
  }

  private void checkFile(String fileUrl) {
    if (isEmpty(fileUrl)) {
      throw new IllegalArgumentException("File URL should be provided");
    }
    if (!s3Service.exists(fileUrl)) {
      throw new NotFoundException("File with provided URL doesn't exist: " + fileUrl);
    }
  }

}
