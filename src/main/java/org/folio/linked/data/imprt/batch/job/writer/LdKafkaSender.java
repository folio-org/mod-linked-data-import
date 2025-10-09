package org.folio.linked.data.imprt.batch.job.writer;

import static org.folio.linked.data.imprt.util.CollectionUtil.chunked;

import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.ImportResult;
import org.folio.spring.tools.kafka.FolioMessageProducer;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class LdKafkaSender implements ItemWriter<Set<Resource>> {

  @Qualifier("importResultMessageProducer")
  private final FolioMessageProducer<ImportResult> importResultFolioMessageProducer;
  @Value("${mod-linked-data-import.result-chunk-size}")
  private Integer chunkSize;

  @Override
  public void write(Chunk<? extends Set<Resource>> chunk) {
    var messages = chunked(chunk.getItems().stream().flatMap(Set::stream), chunkSize)
      .map(ImportResult::new)
      .toList();
    importResultFolioMessageProducer.sendMessages(messages);
  }

}
