package org.folio.linked.data.imprt.batch.job.writer;

import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.folio.ld.dictionary.model.Resource;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class LdKafkaSender implements ItemWriter<Set<Resource>> {

  @Override
  public void write(Chunk<? extends Set<Resource>> chunk) {
    chunk.forEach(r -> log.info("Resource to be sent to Kafka: {}", r));
  }

}
