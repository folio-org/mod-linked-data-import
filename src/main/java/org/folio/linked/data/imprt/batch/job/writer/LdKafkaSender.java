package org.folio.linked.data.imprt.batch.job.writer;

import static org.folio.linked.data.imprt.util.CollectionUtil.chunked;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.ImportOutput;
import org.folio.spring.tools.kafka.FolioMessageProducer;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@StepScope
@RequiredArgsConstructor
public class LdKafkaSender implements ItemWriter<Set<Resource>> {

  @Qualifier("importOutputMessageProducer")
  private final FolioMessageProducer<ImportOutput> importOutputFolioMessageProducer;
  private final ObjectMapper objectMapper;
  @Value("${mod-linked-data-import.output-chunk-size}")
  private Integer chunkSize;

  @Override
  public void write(Chunk<? extends Set<Resource>> chunk) {
    var messages = chunked(chunk.getItems().stream().flatMap(Set::stream), chunkSize)
      .map(serializeResources())
      .map(ImportOutput::new)
      .toList();
    importOutputFolioMessageProducer.sendMessages(messages);
  }

  private @NotNull Function<List<Resource>, List<String>> serializeResources() {
    return resources -> resources.stream()
      .map(r -> {
        try {
          return objectMapper.writeValueAsString(r);
        } catch (JsonProcessingException e) {
          log.error("Error serializing resource with id {}", r.getId(), e);
          return null;
        }
      })
      .filter(Objects::nonNull)
      .toList();
  }

}
