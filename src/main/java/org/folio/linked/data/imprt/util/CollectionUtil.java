package org.folio.linked.data.imprt.util;

import static java.util.LinkedHashSet.newLinkedHashSet;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CollectionUtil {

  public static <T> Stream<Set<T>> chunked(Stream<T> stream, int chunkSize) {
    var sourceIterator = stream.iterator();
    Iterable<Set<T>> iterable = () -> new Iterator<>() {
      @Override
      public boolean hasNext() {
        return sourceIterator.hasNext();
      }

      @Override
      public Set<T> next() {
        Set<T> chunk = newLinkedHashSet(chunkSize);
        int count = 0;
        while (count < chunkSize && sourceIterator.hasNext()) {
          chunk.add(sourceIterator.next());
          count++;
        }
        return chunk;
      }
    };
    return StreamSupport.stream(iterable.spliterator(), false);
  }
}
