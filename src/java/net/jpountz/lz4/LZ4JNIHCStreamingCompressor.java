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

import static net.jpountz.lz4.LZ4Constants.DEFAULT_COMPRESSION_LEVEL;

/**
 * High compression (HC) streaming LZ4 compressor with dictionary support.
 * <p>
 * This compressor uses the HC LZ4 compression algorithm which provides better
 * compression ratio at the cost of speed. It can attach a shared
 * {@link LZ4JNIHCDictionary} for improved compression ratio on small data.
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
 * @see LZ4JNIHCDictionary
 * @see LZ4Factory#highStreamingCompressor()
 */
public final class LZ4JNIHCStreamingCompressor extends LZ4StreamingCompressor {

  private final AtomicLong streamPtr;
  private final int compressionLevel;
  private final Cleanable cleanable;

  /**
   * Creates a new HC streaming compressor with default compression level.
   * Package-private: use {@link LZ4Factory#highStreamingCompressor()}.
   */
  LZ4JNIHCStreamingCompressor() {
    this(DEFAULT_COMPRESSION_LEVEL);
  }

  /**
   * Creates a new HC streaming compressor with specified compression level.
   * Package-private: use {@link LZ4Factory#highStreamingCompressor(int)}.
   *
   * @param compressionLevel compression level (1-17, higher = better compression)
   */
  LZ4JNIHCStreamingCompressor(int compressionLevel) {
    this.compressionLevel = compressionLevel;
    long ptr = LZ4JNI.LZ4_createStreamHC();
    if (ptr == 0) {
      throw new LZ4Exception("Failed to create HC stream");
    }
    this.streamPtr = new AtomicLong(ptr);
    LZ4JNI.LZ4_resetStreamHC_fast(ptr, compressionLevel);
    this.cleanable = ResourceCleanerFactory.getCleaner().register(this, new StreamCleaner(this.streamPtr));
  }

  /**
   * Returns the compression level.
   *
   * @return compression level
   */
  public int getCompressionLevel() {
    return compressionLevel;
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
  public void attachDictionary(LZ4JNIHCDictionary dictionary) {
    long ptr = streamPtr.get();
    if (ptr == 0) {
      throw new IllegalStateException("Compressor has been closed");
    }
    if (dictionary == null) {
      LZ4JNI.LZ4_attach_HC_dictionary(ptr, 0);
    } else {
      LZ4JNI.LZ4_attach_HC_dictionary(ptr, dictionary.getStreamPtr());
    }
  }

  @Override
  public void reset() {
    long ptr = streamPtr.get();
    if (ptr == 0) {
      throw new IllegalStateException("Compressor has been closed");
    }
    LZ4JNI.LZ4_resetStreamHC_fast(ptr, compressionLevel);
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

    int result = LZ4JNI.LZ4_compress_HC_continue(
      ptr, src, null, srcOff, srcLen,
      dest, null, destOff, maxDestLen);

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

    int result = LZ4JNI.LZ4_compress_HC_continue(
      ptr, null, src, srcOff, srcLen,
      null, dest, destOff, maxDestLen);

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
  public int resetAttachDictCompress(LZ4JNIHCDictionary dictionary,
                                     byte[] src, int srcOff, int srcLen,
                                     byte[] dest, int destOff, int maxDestLen) {
    long ptr = streamPtr.get();
    if (ptr == 0) {
      throw new IllegalStateException("Compressor has been closed");
    }
    SafeUtils.checkRange(src, srcOff, srcLen);
    SafeUtils.checkRange(dest, destOff, maxDestLen);

    long dictPtr = (dictionary == null) ? 0 : dictionary.getStreamPtr();

    int result = LZ4JNI.LZ4_reset_attachDictHC_compress(ptr, dictPtr, compressionLevel,
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
    return "LZ4JNIHCStreamingCompressor[level=" + compressionLevel +
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
        LZ4JNI.LZ4_freeStreamHC(ptr);
      }
    }
  }
}

