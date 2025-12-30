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
 * Dictionary for high compression (HC) LZ4 streaming compression.
 * <p>
 * This dictionary can only be used with {@link LZ4JNIHCStreamingCompressor}.
 * It can be safely shared across multiple threads, while each thread uses
 * its own compressor instance.
 * </p>
 * <p>
 * <b>Resource Management:</b> This class holds native memory that must be freed.
 * Always use try-with-resources or explicitly call {@link #close()}.
 * </p>
 *
 * @see LZ4JNIHCStreamingCompressor
 * @see LZ4Factory#highDictionary()
 */
public final class LZ4JNIHCDictionary extends LZ4Dictionary {

    private static final Cleaner CLEANER = Cleaner.create();

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

    private final AtomicLong streamPtr;
    private final ByteBuffer dictDataBuffer;
    private final Cleaner.Cleanable cleanable;

    /**
     * Creates a new HC dictionary with default buffer size (64KB).
     * Package-private: use {@link LZ4Factory#highDictionary()}.
     */
    LZ4JNIHCDictionary() {
        this(DEFAULT_DICT_SIZE);
    }

    /**
     * Creates a new HC dictionary with the specified buffer size.
     * Package-private: use {@link LZ4Factory#highDictionary(int)}.
     *
     * @param dictSize dictionary buffer size, must be positive and <= 64KB
     */
    LZ4JNIHCDictionary(int dictSize) {
        checkDictSize(dictSize);
        long ptr = LZ4JNI.LZ4_createStreamHC();
        if (ptr == 0) {
            throw new LZ4Exception("Failed to create HC stream");
        }
        this.streamPtr = new AtomicLong(ptr);
        this.dictDataBuffer = ByteBuffer.allocateDirect(dictSize);
        this.cleanable = CLEANER.register(this, new StreamCleaner(this.streamPtr));
    }

    /**
     * Loads dictionary data from source array.
     *
     * @param src source array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     */
    @Override
    public void load(byte[] src, int srcOff, int srcLen) {
        SafeUtils.checkRange(src, srcOff, srcLen);
        loadInternal(src, null, srcOff, srcLen);
    }

    /**
     * Loads dictionary data from source buffer.
     *
     * @param src source buffer, must be direct or backed by an array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     */
    @Override
    public void load(ByteBuffer src, int srcOff, int srcLen) {
        ByteBufferUtils.checkRange(src, srcOff, srcLen);
        loadInternal(null, src, srcOff, srcLen);
    }

    private void loadInternal(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen) {
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

        int loaded = LZ4JNI.LZ4_setupDictHC(ptr, dictDataBuffer, srcArray, srcBuffer, srcOff, srcLen);
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
        return "LZ4JNIHCDictionary[closed=" + isClosed() + "]";
    }
}

