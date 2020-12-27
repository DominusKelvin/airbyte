/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.commons.lang;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Queues {

  public static <T> Stream<T> toStream(CloseableQueue<T> queue) {
    return StreamSupport.stream(new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED) {

      @Override
      public boolean tryAdvance(Consumer<? super T> action) {
        // try {
        final T record = queue.poll();
        if (record == null) {
          return false;
        }
        action.accept(record);
        return true;
        // } catch (SQLException e) {
        // throw new RuntimeException(e);
        // }
      }

    }, false);
  }

  // public static <T> Stream<T> toStream(CloseableQueue<byte[]> queue, Function<byte[], T> mapper) {
  // return StreamSupport.stream(new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE,
  // Spliterator.ORDERED) {
  //
  // @Override
  // public boolean tryAdvance(Consumer<? super T> action) {
  //// try {
  // final byte[] record = queue.poll();
  // if (record == null) {
  // return false;
  // }
  // action.accept(mapper.apply(record));
  // return true;
  //// } catch (SQLException e) {
  //// throw new RuntimeException(e);
  //// }
  // }
  //
  // }, false);
  // }
  //
  // public static <T> Stream<T> toJsonStream(CloseableQueue<byte[]> queue, Class<T> klass) {
  // return toStream(queue, bytes -> Jsons.deserialize(new String(bytes, Charsets.UTF_8), klass));
  // }
}
