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

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dictionary for fast LZ4 streaming compression.
 * <p>
 * This dictionary can only be used with {@link LZ4JNIFastStreamingCompressor}.
 * It can be safely shared across multiple threads, while each thread uses
 * its own compressor instance.
 * </p>
 * <p>
 * Fast dictionaries support a "thorough" loading mode that uses more CPU
 * but provides better compression ratio when the dictionary is reused
 * across multiple compression sessions.
 * </p>
 * <p>
 * <b>Resource Management:</b> This class holds native memory that must be freed.
 * Always use try-with-resources or explicitly call {@link #close()}.
 * A Cleaner is registered as a safety net, but explicit cleanup is preferred.
 * </p>
 *
 * @see LZ4JNIFastStreamingCompressor
 * @see LZ4Factory#fastDictionary()
 */
public final class LZ4JNIFastDictionary extends LZ4Dictionary {

    private static final Cleaner CLEANER = Cleaner.create();

    /**
     * Weak reference cleanup action - must not reference the outer class.
     */
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

    private final AtomicLong streamPtr;
    private final ByteBuffer dictDataBuffer;
    private final Cleaner.Cleanable cleanable;

    /**
     * Creates a new fast dictionary with default buffer size (64KB).
     * Package-private: use {@link LZ4Factory#fastDictionary()}.
     */
    LZ4JNIFastDictionary() {
        this(DEFAULT_DICT_SIZE);
    }

    /**
     * Creates a new fast dictionary with the specified buffer size.
     * Package-private: use {@link LZ4Factory#fastDictionary(int)}.
     *
     * @param dictSize dictionary buffer size, must be positive and <= 64KB
     */
    LZ4JNIFastDictionary(int dictSize) {
        checkDictSize(dictSize);
        long ptr = LZ4JNI.LZ4_createStream();
        if (ptr == 0) {
            throw new LZ4Exception("Failed to create stream");
        }
        this.streamPtr = new AtomicLong(ptr);
        this.dictDataBuffer = ByteBuffer.allocateDirect(dictSize);
        this.cleanable = CLEANER.register(this, new StreamCleaner(this.streamPtr));
    }

    /**
     * Loads dictionary data from source array.
     * Uses fast loading mode (equivalent to {@code load(src, srcOff, srcLen, false)}).
     *
     * @param src source array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     */
    @Override
    public void load(byte[] src, int srcOff, int srcLen) {
        load(src, srcOff, srcLen, false);
    }

    /**
     * Loads dictionary data from source buffer.
     * Uses fast loading mode (equivalent to {@code load(src, srcOff, srcLen, false)}).
     *
     * @param src source buffer, must be direct or backed by an array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     */
    @Override
    public void load(ByteBuffer src, int srcOff, int srcLen) {
        load(src, srcOff, srcLen, false);
    }

    /**
     * Loads dictionary data from source array with optional thorough mode.
     * <p>
     * When {@code thorough} is true, uses more CPU to reference dictionary
     * content more thoroughly. This provides better compression ratio when
     * the dictionary is reused across multiple compression sessions.
     * </p>
     *
     * @param src source array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     * @param thorough if true, uses more CPU for better compression ratio
     */
    public void load(byte[] src, int srcOff, int srcLen, boolean thorough) {
        SafeUtils.checkRange(src, srcOff, srcLen);
        loadInternal(src, null, srcOff, srcLen, thorough);
    }

    /**
     * Loads dictionary data from source buffer with optional thorough mode.
     * <p>
     * When {@code thorough} is true, uses more CPU to reference dictionary
     * content more thoroughly. This provides better compression ratio when
     * the dictionary is reused across multiple compression sessions.
     * </p>
     *
     * @param src source buffer, must be direct or backed by an array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     * @param thorough if true, uses more CPU for better compression ratio
     */
    public void load(ByteBuffer src, int srcOff, int srcLen, boolean thorough) {
        ByteBufferUtils.checkRange(src, srcOff, srcLen);
        loadInternal(null, src, srcOff, srcLen, thorough);
    }

    private void loadInternal(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, boolean thorough) {
        long ptr = streamPtr.get();
        if (ptr == 0) {
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

        int loaded = LZ4JNI.LZ4_setupDict(ptr, dictDataBuffer, srcArray, srcBuffer, srcOff, srcLen, thorough);
        if (loaded < 0) {
            throw new LZ4Exception("Failed to load dictionary");
        }
    }

    @Override
    long getStreamPtr() {
        long ptr = streamPtr.get();
        if (ptr == 0) {
            throw new IllegalStateException("Dictionary has been closed");
        }
        return ptr;
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
        return "LZ4JNIFastDictionary[closed=" + isClosed() + "]";
    }
}

