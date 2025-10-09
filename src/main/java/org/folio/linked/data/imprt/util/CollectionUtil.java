package org.folio.linked.data.imprt.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CollectionUtil {

  public static <T> Stream<List<T>> chunked(Stream<T> stream, int chunkSize) {
    var sourceIterator = stream.iterator();
    Iterable<List<T>> iterable = () -> new Iterator<>() {
      @Override
      public boolean hasNext() {
        return sourceIterator.hasNext();
      }

      @Override
      public List<T> next() {
        var chunk = new ArrayList<T>(chunkSize);
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
