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
import net.jpountz.util.SafeUtils;

import java.nio.ByteBuffer;

public abstract class LZ4Dictionary implements AutoCloseable {

    public static LZ4Dictionary create() {
        return new LZ4JNIDictionary(false);
    }

    public static LZ4Dictionary create(int size) {
        return new LZ4JNIDictionary( false, size);
    }

    public static LZ4Dictionary createHC() {
        return new LZ4JNIDictionary(true);
    }

    public static LZ4Dictionary createHC(int size) {
        return new LZ4JNIDictionary(true, size);
    }

    /**
     * Replaces the dictionary with data copied from src and loads it into the stream.
     *
     * @param src source array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     * @param thorough if true, uses more CPU for better compression ratio
     */
    public abstract void load(byte[] src, int srcOff, int srcLen, boolean thorough);

    /**
     * Replaces the dictionary with data copied from src and loads it into the stream.
     *
     * @param src source buffer, must be direct or backed by an array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     * @param thorough if true, uses more CPU for better compression ratio
     */
    public abstract void load(ByteBuffer src, int srcOff, int srcLen, boolean thorough);

    /**
     * Returns true if this is a high compression (HC) dictionary.
     */
    public abstract boolean isHighCompression();

    /**
     * Returns the native stream pointer for use by compressors.
     * Package-private for internal use.
     */
    abstract long getStreamPtr();

    /**
     * Closes this dictionary and releases native resources.
     * After closing, the dictionary cannot be used.
     */
    @Override
    public abstract void close();

    /**
     * Returns true if this dictionary has been closed.
     */
    public abstract boolean isClosed();
}

/**
 * JNI-backed implementation of LZ4Dictionary.
 */
final class LZ4JNIDictionary extends LZ4Dictionary {

    private volatile long streamPtr;
    private final boolean highCompression;
    private final ByteBuffer dictDataBuffer;
    private volatile int dictSize = 0;

    /**
     * Allocates a new LZ4 dictionary with default dictionary buffer length (64KB).
     * <p>
     * Should be followed by a call to {@code load()} to load dictionary data.
     *
     * @param highCompression if true, creates a high compression (HC) dictionary
     */
    LZ4JNIDictionary(boolean highCompression) {
        this(highCompression, 64 << 10); // 64KB
    }

    /**
     * Allocates a new LZ4 dictionary with given dictionary buffer length.
     * <p>
     * Should be followed by a call to {@code load()} to load dictionary data.
     *
     * @param highCompression if true, creates a high compression (HC) dictionary
     * @param dictLen length of dictionary buffer, must be positive and <= 64KB
     */
    LZ4JNIDictionary(boolean highCompression, int dictLen) {
          if (dictLen <= 0) {
              throw new IllegalArgumentException("dictLen must be positive");
          }
          if (dictLen > (64 << 10)) {
              throw new IllegalArgumentException("dictLen must be <= 64KB");
          }

          this.highCompression = highCompression;
          if (highCompression) {
              this.streamPtr = LZ4JNI.LZ4_createStreamHC();
              if (this.streamPtr == 0) {
                  throw new LZ4Exception("Failed to create HC stream");
              }
          } else {
              this.streamPtr = LZ4JNI.LZ4_createStream();
              if (this.streamPtr == 0) {
                  throw new LZ4Exception("Failed to create stream");
              }
          }
          dictDataBuffer = ByteBuffer.allocateDirect(dictLen);
      }

    /**
     * Replaces the dictionary with data copied from src and loads it into the stream.
     *
     * @param src source array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     * @param thorough if true, uses more CPU for better compression ratio
     */
    @Override
    public void load(byte[] src, int srcOff, int srcLen, boolean thorough) {
        SafeUtils.checkRange(src, srcOff, srcLen);
        load(src, null, srcOff, srcLen, thorough);
    }

    /**
     * Replaces the dictionary with data copied from src and loads it into the stream.
     *
     * @param src source buffer, must be direct or backed by an array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     * @param thorough if true, uses more CPU for better compression ratio
     */
    @Override
    public void load(ByteBuffer src, int srcOff, int srcLen, boolean thorough) {
        ByteBufferUtils.checkRange(src, srcOff, srcLen);
        load(null, src, srcOff, srcLen, thorough);
    }

    /**
     * Replaces the dictionary with data copied from src and loads it into the stream.
     *
     * @param srcArray source array
     * @param srcBuffer source buffer, must be direct or backed by an array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     * @param thorough if true, uses more CPU for better compression ratio
     */
    private void load(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, boolean thorough) {
        if (isClosed()) {
            throw new IllegalStateException("Dictionary has been closed");
        }

        if (srcLen > dictDataBuffer.capacity()) {
            throw new IndexOutOfBoundsException("Dictionary buffer too small");
        }

        if (srcBuffer != null && !srcBuffer.isDirect()) {
            if (!srcBuffer.hasArray()) {
                throw new IllegalArgumentException("srcBuffer must be direct or backed by an array");
            }
            srcArray = srcBuffer.array();
            srcOff += srcBuffer.arrayOffset();
            srcBuffer = null;
        }

        int loaded;
        if (highCompression) {
            loaded = LZ4JNI.LZ4_setupDictHC(this.getStreamPtr(), this.dictDataBuffer, srcArray, srcBuffer, srcOff, srcLen);
        } else {
            loaded = LZ4JNI.LZ4_setupDict(this.getStreamPtr(), this.dictDataBuffer, srcArray, srcBuffer, srcOff, srcLen, thorough);
        }
        if (loaded < 0) {
            throw new LZ4Exception("Failed to load dictionary");
        }
        this.dictSize = loaded;
    }

    @Override
    public boolean isHighCompression() {
        return highCompression;
    }

    @Override
    long getStreamPtr() {
        if (isClosed()) {
            throw new IllegalStateException("Dictionary has been closed");
        }
        return streamPtr;
    }

    @Override
    public boolean isClosed() {
        return streamPtr == 0;
    }

    @Override
    public synchronized void close() {
        long ptr = streamPtr;
        if (ptr != 0) {
            streamPtr = 0;
            if (highCompression) {
                LZ4JNI.LZ4_freeStreamHC(ptr);
            } else {
                LZ4JNI.LZ4_freeStream(ptr);
            }
        }
    }

    @Override
    public String toString() {
        return "LZ4Dictionary[" + (highCompression ? "HC" : "fast") + 
               ", closed=" + isClosed() + "]";
    }
}

