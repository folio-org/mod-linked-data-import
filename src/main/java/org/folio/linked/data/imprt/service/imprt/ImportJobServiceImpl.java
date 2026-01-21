package org.folio.linked.data.imprt.service.imprt;

import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static org.folio.linked.data.imprt.batch.job.Parameters.CONTENT_TYPE;
import static org.folio.linked.data.imprt.batch.job.Parameters.DEFAULT_WORK_TYPE;
import static org.folio.linked.data.imprt.batch.job.Parameters.FILE_URL;
import static org.folio.linked.data.imprt.batch.job.Parameters.STARTED_BY;
import static org.springframework.util.ObjectUtils.isEmpty;

import java.util.Properties;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.folio.linked.data.imprt.domain.dto.DefaultWorkType;
import org.folio.linked.data.imprt.service.s3.S3Service;
import org.folio.spring.FolioExecutionContext;
import org.folio.spring.exception.NotFoundException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ImportJobServiceImpl implements ImportJobService {

  private static final String DEFAULT_CONTENT_TYPE = "application/ld+json";
  private final Job rdfImportJob;
  private final JobOperator jobOperator;
  private final S3Service s3Service;
  private final FolioExecutionContext folioExecutionContext;

  @Override
  public Long start(String fileUrl, String contentType, DefaultWorkType defaultWorkType) {
    checkFile(fileUrl);
    var userId = folioExecutionContext.getUserId();
    var params = new Properties();
    params.setProperty(FILE_URL, fileUrl);
    params.setProperty(CONTENT_TYPE, isEmpty(contentType) ? DEFAULT_CONTENT_TYPE : contentType);
    params.setProperty(STARTED_BY, ofNullable(userId).map(UUID::toString).orElse("unknown"));
    params.setProperty("run.timestamp", String.valueOf(System.currentTimeMillis()));
    if (nonNull(defaultWorkType)) {
      params.setProperty(DEFAULT_WORK_TYPE, defaultWorkType.name());
    }

    try {
      return jobOperator.start(rdfImportJob.getName(), params);
    } catch (NoSuchJobException | JobInstanceAlreadyExistsException | JobParametersInvalidException e) {
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
