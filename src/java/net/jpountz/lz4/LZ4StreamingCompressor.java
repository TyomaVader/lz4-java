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
import java.util.Arrays;

/**
 * Base class for streaming LZ4 compressors that support dictionary compression.
 * <p>
 * Streaming compressors can attach a shared dictionary for improved compression
 * ratio on small data. The dictionary can be safely shared across multiple threads,
 * while each thread uses its own compressor instance.
 * </p>
 * <p>
 * Use {@link LZ4Factory#fastStreamingCompressor()} for fast compression or
 * {@link LZ4Factory#highStreamingCompressor()} for high compression.
 * </p>
 * <p>
 * <b>Thread Safety:</b> Each compressor instance must be used by only one thread
 * at a time. However, the attached dictionary can be shared across multiple threads.
 * </p>
 *
 * @see LZ4JNIFastStreamingCompressor
 * @see LZ4JNIHCStreamingCompressor
 * @see LZ4Dictionary
 */
public abstract class LZ4StreamingCompressor implements AutoCloseable {

    /**
     * Resets the compressor for a new compression session.
     * This clears any attached dictionary.
     */
    public abstract void reset();

    /**
     * Returns the maximum compressed length for an input of size {@code length}.
     *
     * @param length the input size in bytes
     * @return the maximum compressed length in bytes
     */
    public static int maxCompressedLength(int length) {
        return LZ4Utils.maxCompressedLength(length);
    }

    /**
     * Compresses the source data.
     * <p>
     * If a dictionary has been attached, it will be used for this compression
     * and then automatically detached.
     * </p>
     *
     * @param src source data
     * @param srcOff offset in source
     * @param srcLen length to compress
     * @param dest destination buffer
     * @param destOff offset in destination
     * @param maxDestLen maximum bytes to write
     * @return compressed size
     * @throws LZ4Exception if compression fails
     */
    public abstract int compress(byte[] src, int srcOff, int srcLen,
                                 byte[] dest, int destOff, int maxDestLen);

    /**
     * Compresses the source data.
     * <p>
     * If a dictionary has been attached, it will be used for this compression
     * and then automatically detached.
     * </p>
     *
     * @param src source data
     * @param srcOff offset in source
     * @param srcLen length to compress
     * @param dest destination buffer
     * @param destOff offset in destination
     * @param maxDestLen maximum bytes to write
     * @return compressed size
     * @throws LZ4Exception if compression fails
     */
    public abstract int compress(ByteBuffer src, int srcOff, int srcLen,
                                 ByteBuffer dest, int destOff, int maxDestLen);

    /**
     * Convenience method for compressing entire arrays.
     *
     * @param src source data
     * @param dest destination buffer
     * @return compressed size
     */
    public final int compress(byte[] src, byte[] dest) {
        return compress(src, 0, src.length, dest, 0, dest.length);
    }

    /**
     * Convenience method for compressing entire buffers.
     *
     * @param src source data
     * @param dest destination buffer
     * @return compressed size
     */
    public final int compress(ByteBuffer src, ByteBuffer dest) {
        return compress(src, 0, src.remaining(), dest, 0, dest.remaining());
    }

    /**
     * Convenience method that allocates and returns compressed data.
     *
     * @param src source data
     * @param srcOff offset in source
     * @param srcLen length to compress
     * @return compressed data
     */
    public final byte[] compress(byte[] src, int srcOff, int srcLen) {
        int maxLen = maxCompressedLength(srcLen);
        byte[] dest = new byte[maxLen];
        int compressedLen = compress(src, srcOff, srcLen, dest, 0, maxLen);
        return Arrays.copyOf(dest, compressedLen);
    }

    /**
     * Convenience method that allocates and returns compressed data.
     *
     * @param src source data
     * @param srcOff offset in source
     * @param srcLen length to compress
     * @return compressed data as direct ByteBuffer
     */
    public final ByteBuffer compress(ByteBuffer src, int srcOff, int srcLen) {
        int maxLen = maxCompressedLength(srcLen);
        ByteBuffer dest = ByteBuffer.allocateDirect(maxLen);
        int compressedLen = compress(src, srcOff, srcLen, dest, 0, maxLen);
        dest.limit(compressedLen);
        return dest;
    }

    /**
     * Convenience method that allocates and returns compressed data.
     *
     * @param src source data
     * @return compressed data
     */
    public final byte[] compress(byte[] src) {
        return compress(src, 0, src.length);
    }

    /**
     * Convenience method that allocates and returns compressed data.
     *
     * @param src source data
     * @return compressed data as direct ByteBuffer
     */
    public final ByteBuffer compress(ByteBuffer src) {
        return compress(src, 0, src.remaining());
    }

    /**
     * Closes this compressor and releases native resources.
     */
    @Override
    public abstract void close();

    /**
     * Returns true if this compressor has been closed.
     *
     * @return true if closed
     */
    public abstract boolean isClosed();

    /**
     * Checks that the compressor is not closed.
     *
     * @throws IllegalStateException if the compressor is closed
     */
    void checkNotClosed() {
        if (isClosed()) {
            throw new IllegalStateException("Compressor has been closed");
        }
    }
}
