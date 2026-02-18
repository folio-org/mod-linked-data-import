package org.folio.linked.data.imprt.batch.job.writer;

import static org.folio.linked.data.imprt.util.CollectionUtil.chunked;

import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.folio.linked.data.imprt.domain.dto.ImportOutputEvent;
import org.folio.linked.data.imprt.domain.dto.ResourceWithLineNumber;
import org.folio.spring.tools.kafka.FolioMessageProducer;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@StepScope
public class LdKafkaSender implements ItemWriter<Set<ResourceWithLineNumber>> {

  private final Long jobExecutionId;
  private final FolioMessageProducer<ImportOutputEvent> importOutputFolioMessageProducer;
  private final Integer chunkSize;

  public LdKafkaSender(@Value("#{stepExecution.jobExecution.id}") Long jobExecutionId,
                       @Qualifier("importOutputMessageProducer") FolioMessageProducer<ImportOutputEvent> producer,
                       @Value("${mod-linked-data-import.output-chunk-size}") Integer chunkSize) {
    this.jobExecutionId = jobExecutionId;
    this.importOutputFolioMessageProducer = producer;
    this.chunkSize = chunkSize;
  }


  @Override
  public void write(Chunk<? extends Set<ResourceWithLineNumber>> chunk) {
    var messages = chunked(chunk.getItems().stream().flatMap(Set::stream), chunkSize)
      .map(resourcesWithLineNumbers ->
        new ImportOutputEvent(resourcesWithLineNumbers).jobExecutionId(jobExecutionId))
      .toList();
    importOutputFolioMessageProducer.sendMessages(messages);
  }

}
