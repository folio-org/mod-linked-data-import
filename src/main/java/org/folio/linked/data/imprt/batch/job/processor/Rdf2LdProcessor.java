package org.folio.linked.data.imprt.batch.job.processor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toCollection;
import static org.folio.linked.data.imprt.batch.job.Parameters.CONTENT_TYPE;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashSet;
import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.domain.dto.ResourceWithLineNumber;
import org.folio.linked.data.imprt.model.RdfLineWithNumber;
import org.folio.linked.data.imprt.model.entity.FailedRdfLine;
import org.folio.linked.data.imprt.repo.FailedRdfLineRepo;
import org.folio.rdf4ld.service.Rdf4LdService;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@StepScope
public class Rdf2LdProcessor implements ItemProcessor<RdfLineWithNumber, Set<ResourceWithLineNumber>> {

  private static final String EMPTY_RESULT = "Empty result returned by rdf4ld library";
  private final Long jobExecutionId;
  private final String contentType;
  private final Rdf4LdService rdf4LdService;
  private final FailedRdfLineRepo failedRdfLineRepo;

  public Rdf2LdProcessor(@Value("#{stepExecution.jobExecution.id}") Long jobExecutionId,
                         @Value("#{jobParameters['" + CONTENT_TYPE + "']}") String contentType,
                         Rdf4LdService rdf4LdService,
                         FailedRdfLineRepo failedRdfLineRepo) {
    this.jobExecutionId = jobExecutionId;
    this.contentType = contentType;
    this.rdf4LdService = rdf4LdService;
    this.failedRdfLineRepo = failedRdfLineRepo;
  }

  @Override
  @Nullable
  @SuppressWarnings("java:S2638")  // false positive "Method override should not change contracts"
  public Set<ResourceWithLineNumber> process(@NonNull RdfLineWithNumber rdfLineWithNumber) {
    var rdfLine = rdfLineWithNumber.getContent();
    var lineNumber = rdfLineWithNumber.getLineNumber();
    log.trace("Processing RDF line #{} of contentType[{}]: {}", lineNumber, contentType, rdfLine);
    try {
      var is = new ByteArrayInputStream(rdfLine.getBytes(UTF_8));
      var result = rdf4LdService.mapBibframe2RdfToLd(is, contentType);
      if (result.isEmpty()) {
        log.debug(EMPTY_RESULT + ", saving FailedRdfLine. JobExecutionId [{}], line #{}", jobExecutionId, lineNumber);
        saveFailedLine(lineNumber, rdfLine, EMPTY_RESULT);
        return null;
      }
      return result.stream()
        .map(resource -> new ResourceWithLineNumber(lineNumber, resource))
        .collect(toCollection(LinkedHashSet::new));
    } catch (Exception e) {
      log.warn("Exception during processing RDF line #{}, saving FailedRdfLine. JobExecutionId [{}]", lineNumber,
        jobExecutionId);
      saveFailedLine(lineNumber, rdfLine, e.getMessage());
      return null;
    }
  }

  private void saveFailedLine(long lineNumber, String rdfLine, String message) {
    var frl = new FailedRdfLine()
      .setJobExecutionId(jobExecutionId)
      .setLineNumber(lineNumber)
      .setDescription(message)
      .setFailedRdfLine(rdfLine);
    failedRdfLineRepo.save(frl);
  }
}
