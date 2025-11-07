package org.folio.linked.data.imprt.batch.job.processor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.folio.linked.data.imprt.batch.job.Parameters.CONTENT_TYPE;

import java.io.ByteArrayInputStream;
import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.model.FailedRdfLine;
import org.folio.linked.data.imprt.model.FailedRdfLineRepo;
import org.folio.rdf4ld.service.Rdf4LdService;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@StepScope
public class Rdf2LdProcessor implements ItemProcessor<String, Set<Resource>> {

  private final Long jobInstanceId;
  private final String contentType;
  private final Rdf4LdService rdf4LdService;
  private final FailedRdfLineRepo failedRdfLineRepo;

  public Rdf2LdProcessor(@Value("#{jobInstanceId}") Long jobInstanceId,
                         @Value("#{jobParameters['" + CONTENT_TYPE + "']}") String contentType,
                         Rdf4LdService rdf4LdService,
                         FailedRdfLineRepo failedRdfLineRepo) {
    this.jobInstanceId = jobInstanceId;
    this.contentType = contentType;
    this.rdf4LdService = rdf4LdService;
    this.failedRdfLineRepo = failedRdfLineRepo;
  }


  @Override
  public Set<Resource> process(@NotNull String rdfLine) {
    log.trace("Processing RDF line of contentType[{}]: {}", contentType, rdfLine);
    try {
      var is = new ByteArrayInputStream(rdfLine.getBytes(UTF_8));
      return rdf4LdService.mapBibframe2RdfToLd(is, contentType);
    } catch (Exception e) {
      var frl = new FailedRdfLine()
        .setJobInstanceId(jobInstanceId)
        .setException(e.getMessage())
        .setFailedRdfLine(rdfLine);
      log.warn("Exception during processing RDF line, saving FailedRdfLine. JobInstanceId [{}]", jobInstanceId);
      failedRdfLineRepo.save(frl);
      return Set.of();
    }
  }
}
