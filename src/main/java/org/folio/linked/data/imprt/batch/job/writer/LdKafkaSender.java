package org.folio.linked.data.imprt.batch.job.writer;

import static org.folio.linked.data.imprt.util.CollectionUtil.chunked;

import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.ImportOutput;
import org.folio.spring.tools.kafka.FolioMessageProducer;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@StepScope
public class LdKafkaSender implements ItemWriter<Set<Resource>> {

  private final Long jobInstanceId;
  private final FolioMessageProducer<ImportOutput> importOutputFolioMessageProducer;
  private final Integer chunkSize;

  public LdKafkaSender(@Value("#{jobInstanceId}") Long jobInstanceId,
                       @Qualifier("importOutputMessageProducer") FolioMessageProducer<ImportOutput> producer,
                       @Value("${mod-linked-data-import.output-chunk-size}") Integer chunkSize) {
    this.jobInstanceId = jobInstanceId;
    this.importOutputFolioMessageProducer = producer;
    this.chunkSize = chunkSize;
  }


  @Override
  public void write(Chunk<? extends Set<Resource>> chunk) {
    var messages = chunked(chunk.getItems().stream().flatMap(Set::stream), chunkSize)
      .map(resources -> new ImportOutput(resources).jobInstanceId(jobInstanceId))
      .toList();
    importOutputFolioMessageProducer.sendMessages(messages);
  }

}
