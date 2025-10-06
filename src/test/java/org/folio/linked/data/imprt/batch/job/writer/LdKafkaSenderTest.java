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
import org.folio.ld.dictionary.model.Resource;
import org.folio.linked.data.imprt.domain.dto.ImportResult;
import org.folio.spring.testing.type.UnitTest;
import org.folio.spring.tools.kafka.FolioMessageProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.item.Chunk;
import org.springframework.test.util.ReflectionTestUtils;

@UnitTest
@ExtendWith(MockitoExtension.class)
class LdKafkaSenderTest {

  @Mock
  private FolioMessageProducer<ImportResult> importResultFolioMessageProducer;
  @InjectMocks
  private LdKafkaSender sender;

  @BeforeEach
  void init() {
    ReflectionTestUtils.setField(sender, "chunkSize", 4);
  }

  @Test
  void write_shouldSendEmptyList_ifChunkEmpty() {
    // given
    var emptyChunk = new Chunk<Set<Resource>>();
    var captor = ArgumentCaptor.forClass(List.class);

    // when
    sender.write(emptyChunk);

    // then
    verify(importResultFolioMessageProducer, times(1)).sendMessages(captor.capture());
    var sent = captor.getValue();
    assertThat(sent).isEmpty();
  }

  @Test
  void write_shouldSendSingleMessage_ifResourcesLessOrEqualChunkSize() {
    // given
    var chunk = new Chunk<Set<Resource>>();
    var set = range(0, 2)
      .mapToObj(i -> mock(Resource.class))
      .collect(toCollection(HashSet::new));
    chunk.add(set);
    var captor = ArgumentCaptor.forClass(List.class);

    // when
    sender.write(chunk);

    // then
    verify(importResultFolioMessageProducer, times(1)).sendMessages(captor.capture());
    var messages = captor.getValue();
    assertThat(messages).hasSize(1);
    var msg = (ImportResult) messages.get(0);
    assertThat(msg.getResources()).hasSize(2);
  }

  @Test
  void write_shouldSplitIntoMultipleMessages_ifResourcesExceedChunkSize() {
    // given
    var total = 11; // => 3 messages (4,4,3)
    var chunk = new Chunk<Set<Resource>>();
    var resources = range(0, total)
      .mapToObj(i -> mock(Resource.class))
      .collect(toCollection(ArrayList::new));
    var set1 = new HashSet<>(resources.subList(0, 6));
    var set2 = new HashSet<>(resources.subList(6, total));
    chunk.add(set1);
    chunk.add(set2);
    var captor = ArgumentCaptor.forClass(List.class);

    // when
    sender.write(chunk);

    // then
    verify(importResultFolioMessageProducer, times(1)).sendMessages(captor.capture());
    var messages = captor.getValue();
    assertThat(messages).hasSize(3);
    assertThat(((ImportResult) messages.get(0)).getResources()).hasSize(4);
    assertThat(((ImportResult) messages.get(1)).getResources()).hasSize(4);
    assertThat(((ImportResult) messages.get(2)).getResources()).hasSize(3);
  }
}
