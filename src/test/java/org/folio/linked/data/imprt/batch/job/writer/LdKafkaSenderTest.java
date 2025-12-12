package org.folio.linked.data.imprt.batch.job.writer;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.SneakyThrows;
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.ImportOutputEvent;
import org.folio.linked.data.imprt.domain.dto.ResourceWithLineNumber;
import org.folio.spring.testing.type.UnitTest;
import org.folio.spring.tools.kafka.FolioMessageProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.item.Chunk;

@UnitTest
@ExtendWith(MockitoExtension.class)
class LdKafkaSenderTest {

  private static final Long JOB_INSTANCE_ID = 1L;
  private static final Integer CHUNK_SIZE = 4;
  @Mock
  private FolioMessageProducer<ImportOutputEvent> importOutputFolioMessageProducer;
  private LdKafkaSender sender;

  @SneakyThrows
  @BeforeEach
  void init() {
    sender = new LdKafkaSender(JOB_INSTANCE_ID, importOutputFolioMessageProducer, CHUNK_SIZE);
  }

  @Test
  void write_shouldSendEmptyList_ifChunkEmpty() {
    // given
    var emptyChunk = new Chunk<Set<ResourceWithLineNumber>>();
    var captor = ArgumentCaptor.forClass(List.class);

    // when
    sender.write(emptyChunk);

    // then
    verify(importOutputFolioMessageProducer, times(1)).sendMessages(captor.capture());
    var sent = captor.getValue();
    assertThat(sent).isEmpty();
  }

  @Test
  void write_shouldSendSingleMessage_ifResourcesLessOrEqualChunkSize() {
    // given
    var chunk = new Chunk<Set<ResourceWithLineNumber>>();
    var set = range(0, 2)
      .mapToObj(i -> new ResourceWithLineNumber(i + 1L, mock(Resource.class)))
      .collect(toCollection(HashSet::new));
    chunk.add(set);
    var captor = ArgumentCaptor.forClass(List.class);

    // when
    sender.write(chunk);

    // then
    verify(importOutputFolioMessageProducer, times(1)).sendMessages(captor.capture());
    var messages = captor.getValue();
    assertThat(messages).hasSize(1);
    var msg = (ImportOutputEvent) messages.get(0);
    assertThat(msg.getResourcesWithLineNumbers()).hasSize(2);
    assertThat(msg.getJobInstanceId()).isEqualTo(JOB_INSTANCE_ID);
  }

  @Test
  void write_shouldSplitIntoMultipleMessages_ifResourcesExceedChunkSize() {
    // given
    var total = 11;
    var chunk = new Chunk<Set<ResourceWithLineNumber>>();
    var resources = range(0, total)
      .mapToObj(i -> new ResourceWithLineNumber(i + 1L, mock(Resource.class)))
      .collect(toCollection(ArrayList::new));
    var set1 = new HashSet<>(resources.subList(0, 6));
    var set2 = new HashSet<>(resources.subList(6, total));
    chunk.add(set1);
    chunk.add(set2);
    var captor = ArgumentCaptor.forClass(List.class);

    // when
    sender.write(chunk);

    // then
    verify(importOutputFolioMessageProducer, times(1)).sendMessages(captor.capture());
    var messages = captor.getValue();
    assertThat(messages).hasSize(3);
    assertThat(((ImportOutputEvent) messages.get(0)).getResourcesWithLineNumbers()).hasSize(4);
    assertThat(((ImportOutputEvent) messages.get(1)).getResourcesWithLineNumbers()).hasSize(4);
    assertThat(((ImportOutputEvent) messages.get(2)).getResourcesWithLineNumbers()).hasSize(3);
  }
}
