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

import java.nio.ByteBuffer;

/**
 * Base class for LZ4 dictionaries used with streaming compression.
 * <p>
 * Dictionaries enable better compression ratios for small data by providing
 * a shared context. A dictionary can be safely shared across multiple threads,
 * while each thread uses its own streaming compressor.
 * </p>
 * <p>
 * Use {@link LZ4Factory#fastDictionary()} for fast compression or
 * {@link LZ4Factory#highDictionary()} for high compression.
 * </p>
 *
 * @see LZ4JNIFastDictionary
 * @see LZ4JNIHCDictionary
 * @see LZ4JNIFastStreamingCompressor
 * @see LZ4JNIHCStreamingCompressor
 */
public abstract class LZ4Dictionary implements AutoCloseable {

    /** Maximum dictionary size (64KB). */
    public static final int MAX_DICT_SIZE = 64 * 1024;

    /** Default dictionary size (64KB). */
    public static final int DEFAULT_DICT_SIZE = MAX_DICT_SIZE;

    /**
     * Replaces the dictionary with data copied from src and loads it into the stream.
     *
     * @param src source array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     */
    public abstract void load(byte[] src, int srcOff, int srcLen);

    /**
     * Replaces the dictionary with data copied from src and loads it into the stream.
     *
     * @param src source buffer, must be direct or backed by an array
     * @param srcOff offset in source
     * @param srcLen length of source, must not exceed dictionary buffer capacity
     */
    public abstract void load(ByteBuffer src, int srcOff, int srcLen);

    /**
     * Convenience method to load dictionary from entire array.
     *
     * @param src source array
     */
    public final void load(byte[] src) {
        load(src, 0, src.length);
    }

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
     *
     * @return true if this dictionary has been closed
     */
    public abstract boolean isClosed();

    /**
     * Checks that the dictionary buffer size is valid.
     *
     * @param dictSize the dictionary size to validate
     * @throws IllegalArgumentException if size is invalid
     */
    static void checkDictSize(int dictSize) {
        if (dictSize <= 0) {
            throw new IllegalArgumentException("dictSize must be positive");
        }
        if (dictSize > MAX_DICT_SIZE) {
            throw new IllegalArgumentException("dictSize must be <= " + MAX_DICT_SIZE);
        }
    }
}
