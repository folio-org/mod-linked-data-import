package org.folio.linked.data.imprt.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.folio.spring.testing.type.UnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@UnitTest
@ExtendWith(MockitoExtension.class)
class CollectionUtilTest {

  @Test
  void chunked_shouldReturnEmptyStream_ifSourceEmpty() {
    // given
    var empty = Stream.<Integer>empty();

    // when
    var result = CollectionUtil.chunked(empty, 5).toList();

    // then
    assertThat(result).isEmpty();
  }

  @Test
  void chunked_shouldReturnSingleChunk_ifChunkSizeGreaterThanElements() {
    // given
    var stream = Stream.of(1, 2, 3);

    // when
    var result = CollectionUtil.chunked(stream, 10).toList();

    // then
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).containsExactly(1, 2, 3);
  }

  @Test
  void chunked_shouldSplitStreamIntoEqualChunks_andLastRemainder() {
    // given
    var source = Stream.iterate(0, i -> i + 1).limit(10);

    // when
    var result = CollectionUtil.chunked(source, 3).toList();

    // then
    assertThat(result).hasSize(4);
    assertThat(result.get(0)).containsOnly(0, 1, 2);
    assertThat(result.get(1)).containsOnly(3, 4, 5);
    assertThat(result.get(2)).containsOnly(6, 7, 8);
    assertThat(result.get(3)).containsOnly(9);
  }

  @Test
  void chunked_shouldHandleChunkSizeOne() {
    // given
    var stream = Stream.of("a", "b");

    // when
    var result = CollectionUtil.chunked(stream, 1).toList();

    // then
    assertThat(result).hasSize(2);
    assertThat(result.get(0)).containsOnly("a");
    assertThat(result.get(1)).containsOnly("b");
  }

  @Test
  void chunked_shouldNotReuseListInstances() {
    // given
    var stream = Stream.of(1, 2, 3, 4);

    // when
    var result = CollectionUtil.chunked(stream, 2).toList();

    // then
    assertThat(result.get(0)).isNotSameAs(result.get(1));
  }

  @Test
  void chunked_shouldPreserveOrder() {
    // given
    var stream = Stream.of(1, 2, 3, 4, 5, 6, 7);

    // when
    var result = CollectionUtil.chunked(stream, 3).toList();

    // then
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).containsExactly(1, 2, 3);
    assertThat(result.get(1)).containsExactly(4, 5, 6);
    assertThat(result.get(2)).containsExactly(7);
  }
}
