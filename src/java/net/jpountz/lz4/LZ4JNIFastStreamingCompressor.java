package net.jpountz.lz4;

/*
 * Copyright 2020 Adrien Grand and the lz4-java contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import net.jpountz.util.ByteBufferUtils;
import net.jpountz.util.Cleanable;
import net.jpountz.util.ResourceCleanerFactory;
import net.jpountz.util.SafeUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fast streaming LZ4 compressor with dictionary support.
 * <p>
 * This compressor uses the fast LZ4 compression algorithm. It can attach a
 * shared {@link LZ4JNIFastDictionary} for improved compression ratio on small data.
 * </p>
 * <p>
 * <b>Thread Safety:</b> Each compressor instance must be used by only one thread
 * at a time. However, the attached dictionary can be shared across multiple threads.
 * </p>
 * <p>
 * <b>Resource Management:</b> This class holds native memory that must be freed.
 * Always use try-with-resources or explicitly call {@link #close()}.
 * </p>
 *
 * @see LZ4JNIFastDictionary
 * @see LZ4Factory#fastStreamingCompressor()
 */
public final class LZ4JNIFastStreamingCompressor extends LZ4StreamingCompressor {

  private final AtomicLong streamPtr;
  private final int acceleration;
  private final Cleanable cleanable;

  /**
   * Creates a new fast streaming compressor with default acceleration.
   * Package-private: use {@link LZ4Factory#fastStreamingCompressor()}.
   */
  LZ4JNIFastStreamingCompressor() {
    this(1);
  }

  /**
   * Creates a new fast streaming compressor with specified acceleration.
   * Package-private: use {@link LZ4Factory#fastStreamingCompressor(int)}.
   *
   * @param acceleration acceleration factor (1 = default, higher = faster but less compression)
   */
  LZ4JNIFastStreamingCompressor(int acceleration) {
    this.acceleration = acceleration;
    long ptr = LZ4JNI.LZ4_createStream();
    if (ptr == 0) {
      throw new LZ4Exception("Failed to create stream");
    }
    this.streamPtr = new AtomicLong(ptr);
    this.cleanable = ResourceCleanerFactory.getCleaner().register(this, new StreamCleaner(this.streamPtr));
  }

  /**
   * Returns the acceleration factor.
   *
   * @return acceleration factor
   */
  public int getAcceleration() {
    return acceleration;
  }

  /**
   * Attaches a shared dictionary to this compressor for the next compression.
   * <p>
   * The dictionary is used for the next compression call only, then automatically
   * detached. Call this method before each compression if you want to use the
   * dictionary for every compression.
   * </p>
   *
   * @param dictionary the shared dictionary to attach, or null to detach
   */
  public void attachDictionary(LZ4JNIFastDictionary dictionary) {
    long ptr = streamPtr.get();
    if (ptr == 0) {
      throw new IllegalStateException("Compressor has been closed");
    }
    if (dictionary == null) {
      LZ4JNI.LZ4_attach_dictionary(ptr, 0);
    } else {
      LZ4JNI.LZ4_attach_dictionary(ptr, dictionary.getStreamPtr());
    }
  }

  @Override
  public void reset() {
    long ptr = streamPtr.get();
    if (ptr == 0) {
      throw new IllegalStateException("Compressor has been closed");
    }
    LZ4JNI.LZ4_resetStream_fast(ptr);
  }

  @Override
  public int compress(byte[] src, int srcOff, int srcLen,
                      byte[] dest, int destOff, int maxDestLen) {
    long ptr = streamPtr.get();
    if (ptr == 0) {
      throw new IllegalStateException("Compressor has been closed");
    }
    SafeUtils.checkRange(src, srcOff, srcLen);
    SafeUtils.checkRange(dest, destOff, maxDestLen);

    int result = LZ4JNI.LZ4_compress_fast_continue(
      ptr, src, null, srcOff, srcLen,
      dest, null, destOff, maxDestLen, acceleration);

    if (result <= 0) {
      throw new LZ4Exception("Compression failed");
    }
    return result;
  }

  @Override
  public int compress(ByteBuffer src, int srcOff, int srcLen,
                      ByteBuffer dest, int destOff, int maxDestLen) {
    long ptr = streamPtr.get();
    if (ptr == 0) {
      throw new IllegalStateException("Compressor has been closed");
    }
    ByteBufferUtils.checkRange(src, srcOff, srcLen);
    ByteBufferUtils.checkRange(dest, destOff, maxDestLen);

    int result = LZ4JNI.LZ4_compress_fast_continue(
      ptr, null, src, srcOff, srcLen,
      null, dest, destOff, maxDestLen, acceleration);

    if (result <= 0) {
      throw new LZ4Exception("Compression failed");
    }
    return result;
  }

  /**
   * Resets the compressor, attaches the given dictionary, and compresses the data
   * in a single optimized JNI call.
   *
   * @param dictionary the dictionary to attach, or null for no dictionary
   * @param src        source data
   * @param srcOff     offset in source
   * @param srcLen     length to compress
   * @param dest       destination buffer
   * @param destOff    offset in destination
   * @param maxDestLen maximum bytes to write
   * @return compressed size
   * @throws LZ4Exception if compression fails
   */
  public int resetAttachDictCompress(LZ4JNIFastDictionary dictionary,
                                     byte[] src, int srcOff, int srcLen,
                                     byte[] dest, int destOff, int maxDestLen) {
    long ptr = streamPtr.get();
    if (ptr == 0) {
      throw new IllegalStateException("Compressor has been closed");
    }
    SafeUtils.checkRange(src, srcOff, srcLen);
    SafeUtils.checkRange(dest, destOff, maxDestLen);

    long dictPtr = (dictionary == null) ? 0 : dictionary.getStreamPtr();

    int result = LZ4JNI.LZ4_reset_attachDict_compress(ptr, dictPtr, acceleration,
      src, null, srcOff, srcLen,
      dest, null, destOff, maxDestLen);

    if (result <= 0) {
      throw new LZ4Exception("Compression failed");
    }
    return result;
  }

  @Override
  public boolean isClosed() {
    return streamPtr.get() == 0;
  }

  @Override
  public void close() {
    cleanable.clean();
  }

  @Override
  public String toString() {
    return "LZ4JNIFastStreamingCompressor[acceleration=" + acceleration +
      ", closed=" + isClosed() + "]";
  }

  private static class StreamCleaner implements Runnable {
    private final AtomicLong streamPtr;

    StreamCleaner(AtomicLong streamPtr) {
      this.streamPtr = streamPtr;
    }

    @Override
    public void run() {
      long ptr = streamPtr.getAndSet(0);
      if (ptr != 0) {
        LZ4JNI.LZ4_freeStream(ptr);
      }
    }
  }
}

