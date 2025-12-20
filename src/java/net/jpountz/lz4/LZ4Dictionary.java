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
 * A preloaded LZ4 compression dictionary that can be shared across threads.
 * <p>
 * LZ4 1.10.0+ supports efficient dictionary reuse through the 
 * {@code LZ4_attach_dictionary()} API. This class wraps a dictionary stream
 * that has been loaded with dictionary data and can be safely shared across
 * multiple threads for concurrent compression operations.
 * </p>
 * <p>
 * <b>Thread Safety:</b> After creation, an {@code LZ4Dictionary} is READ-ONLY
 * and can be safely shared and used by multiple threads concurrently. Each
 * thread should have its own {@link LZ4StreamingCompressor} that attaches
 * this shared dictionary.
 * </p>
 *
 * @see LZ4StreamingCompressor
 */
public abstract class LZ4Dictionary implements AutoCloseable {

    /**
     * Creates a new LZ4 dictionary for fast compression.
     * <p>
     * The dictionary data is loaded into an internal stream structure.
     * Only the last 64KB of dictionary data is used.
     * </p>
     * 
     * @param dictionary the dictionary data
     * @param thorough if true, uses more CPU to analyze dictionary content
     *                 more thoroughly, resulting in better compression ratio.
     *                 Recommended when dictionary will be reused many times.
     *                 (Uses LZ4_loadDictSlow vs LZ4_loadDict)
     * @return a new shared dictionary
     */
    public static LZ4Dictionary create(byte[] dictionary, boolean thorough) {
        return create(dictionary, 0, dictionary.length, thorough);
    }


    /**
     * Creates a new LZ4 dictionary for fast compression.
     * <p>
     * The dictionary data is loaded into an internal stream structure.
     * Only the last 64KB of dictionary data is used.
     * </p>
     *
     * @param dictionary the dictionary data
     * @param thorough if true, uses more CPU to analyze dictionary content
     *                 more thoroughly, resulting in better compression ratio.
     *                 Recommended when dictionary will be reused many times.
     *                 (Uses LZ4_loadDictSlow vs LZ4_loadDict)
     * @return a new shared dictionary
     */
    public static LZ4Dictionary create(ByteBuffer dictionary, boolean thorough) {
        return new LZ4JNIDictionary(dictionary, 0, dictionary.remaining(), thorough, false);
    }

    /**
     * Creates a new LZ4 dictionary for fast compression.
     * 
     * @param dictionary the dictionary data
     * @param offset offset in dictionary array
     * @param length length of dictionary data
     * @param thorough if true, uses more CPU for better compression ratio
     * @return a new shared dictionary
     */
    public static LZ4Dictionary create(byte[] dictionary, int offset, int length, boolean thorough) {
        return new LZ4JNIDictionary(dictionary, offset, length, thorough, false);
    }

    /**
     * Creates a new LZ4 dictionary for fast compression.
     *
     * @param dictionary the dictionary data
     * @param offset offset in dictionary array
     * @param length length of dictionary data
     * @param thorough if true, uses more CPU for better compression ratio
     * @return a new shared dictionary
     */
    public static LZ4Dictionary create(ByteBuffer dictionary, int offset, int length, boolean thorough) {
        return new LZ4JNIDictionary(dictionary, offset, length, thorough, false);
    }

    /**
     * Creates a new LZ4 dictionary for high compression (HC).
     * 
     * @param dictionary the dictionary data
     * @return a new shared HC dictionary
     */
    public static LZ4Dictionary createHC(byte[] dictionary) {
        return createHC(dictionary, 0, dictionary.length);
    }

    /**
     * Creates a new LZ4 dictionary for high compression (HC).
     *
     * @param dictionary the dictionary data
     * @return a new shared HC dictionary
     */
    public static LZ4Dictionary createHC(ByteBuffer dictionary) {
      return createHC(dictionary, 0, dictionary.remaining());
    }

    /**
     * Creates a new LZ4 dictionary for high compression (HC).
     * 
     * @param dictionary the dictionary data
     * @param offset offset in dictionary array
     * @param length length of dictionary data
     * @return a new shared HC dictionary
     */
    public static LZ4Dictionary createHC(byte[] dictionary, int offset, int length) {
        return new LZ4JNIDictionary(dictionary, offset, length, false, true);
    }

    /**
     * Creates a new LZ4 dictionary for high compression (HC).
     *
     * @param dictionary the dictionary data
     * @param offset offset in dictionary array
     * @param length length of dictionary data
     * @return a new shared HC dictionary
     */
    public static LZ4Dictionary createHC(ByteBuffer dictionary, int offset, int length) {
        return new LZ4JNIDictionary(dictionary, offset, length, false, true);
    }

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

    LZ4JNIDictionary(byte[] dictionary, int offset, int length, boolean thorough, boolean highCompression) {
        this.highCompression = highCompression;
        
        if (highCompression) {
            this.streamPtr = LZ4JNI.LZ4_createStreamHC();
            if (this.streamPtr == 0) {
                throw new LZ4Exception("Failed to create HC stream");
            }
            int loaded = LZ4JNI.LZ4_loadDictHC(this.streamPtr, dictionary, null, offset, length);
            if (loaded < 0) {
                LZ4JNI.LZ4_freeStreamHC(this.streamPtr);
                throw new LZ4Exception("Failed to load HC dictionary");
            }
        } else {
            this.streamPtr = LZ4JNI.LZ4_createStream();
            if (this.streamPtr == 0) {
                throw new LZ4Exception("Failed to create stream");
            }
            int loaded;
            if (thorough) {
                loaded = LZ4JNI.LZ4_loadDictSlow(this.streamPtr, dictionary, null, offset, length);
            } else {
                loaded = LZ4JNI.LZ4_loadDict(this.streamPtr, dictionary, null, offset, length);
            }
            if (loaded < 0) {
                LZ4JNI.LZ4_freeStream(this.streamPtr);
                throw new LZ4Exception("Failed to load dictionary");
            }
        }
    }

    LZ4JNIDictionary(ByteBuffer dictionary, int offset, int length, boolean thorough, boolean highCompression) {
        this.highCompression = highCompression;

        if (highCompression) {
            this.streamPtr = LZ4JNI.LZ4_createStreamHC();
            if (this.streamPtr == 0) {
                throw new LZ4Exception("Failed to create HC stream");
            }
            int loaded = LZ4JNI.LZ4_loadDictHC(this.streamPtr, null, dictionary, offset, length);
            if (loaded < 0) {
                LZ4JNI.LZ4_freeStreamHC(this.streamPtr);
                throw new LZ4Exception("Failed to load HC dictionary");
            }
        } else {
            this.streamPtr = LZ4JNI.LZ4_createStream();
            if (this.streamPtr == 0) {
                throw new LZ4Exception("Failed to create stream");
            }
            int loaded;
            if (thorough) {
                loaded = LZ4JNI.LZ4_loadDictSlow(this.streamPtr, null, dictionary, offset, length);
            } else {
                loaded = LZ4JNI.LZ4_loadDict(this.streamPtr, null, dictionary, offset, length);
            }
            if (loaded < 0) {
                LZ4JNI.LZ4_freeStream(this.streamPtr);
                throw new LZ4Exception("Failed to load dictionary");
            }
        }
    }

    @Override
    public boolean isHighCompression() {
        return highCompression;
    }

    @Override
    long getStreamPtr() {
        long ptr = streamPtr;
        if (ptr == 0) {
            throw new IllegalStateException("Dictionary has been closed");
        }
        return ptr;
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

