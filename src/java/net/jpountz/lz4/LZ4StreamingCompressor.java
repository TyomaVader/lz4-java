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

import java.nio.ByteBuffer;
import java.util.Arrays;

import static net.jpountz.util.SafeUtils.checkRange;

/**
 * A streaming LZ4 compressor that supports thread-safe dictionary compression.
 * <p>
 * This compressor can attach a shared {@link LZ4Dictionary} for improved
 * compression ratio on small data. The dictionary can be safely shared across
 * multiple threads, while each thread uses its own {@code LZ4StreamingCompressor}.
 * </p>
 * <p>
 * <b>Thread Safety:</b> Each {@code LZ4StreamingCompressor} instance must be
 * used by only one thread at a time. However, the attached {@link LZ4Dictionary}
 * can be shared across multiple threads.
 * </p>
 *
 * @see LZ4Dictionary
 */
public abstract class LZ4StreamingCompressor implements AutoCloseable {

    /**
     * Creates a new fast streaming compressor.
     * 
     * @return a new fast streaming compressor
     */
    public static LZ4StreamingCompressor create() {
        return create(1);
    }

    /**
     * Creates a new fast streaming compressor with specified acceleration.
     * 
     * @param acceleration acceleration factor (1 = default, higher = faster but less compression)
     * @return a new fast streaming compressor
     */
    public static LZ4StreamingCompressor create(int acceleration) {
        return new LZ4JNIStreamingCompressor(acceleration);
    }

    /**
     * Creates a new high compression (HC) streaming compressor.
     * 
     * @return a new HC streaming compressor with default compression level
     */
    public static LZ4StreamingCompressor createHC() {
        return createHC(LZ4Constants.DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new high compression (HC) streaming compressor.
     * 
     * @param compressionLevel compression level (1-12, higher = better compression)
     * @return a new HC streaming compressor
     */
    public static LZ4StreamingCompressor createHC(int compressionLevel) {
        return new LZ4JNIStreamingCompressorHC(compressionLevel);
    }

    /**
     * Attaches a shared dictionary to this compressor for the next compression.
     * <p>
     * The dictionary is used for the next compression call only, then automatically
     * detached. Call this method before each compression if you want to use the
     * dictionary for every compression.
     * </p>
     * <p>
     * The dictionary must match the compressor type: fast dictionaries for fast
     * compressors, HC dictionaries for HC compressors.
     * </p>
     * 
     * @param dictionary the shared dictionary to attach, or null to detach
     * @throws IllegalArgumentException if dictionary type doesn't match compressor type
     */
    public abstract void attachDictionary(LZ4Dictionary dictionary);

    /**
     * Resets the compressor for a new compression session.
     * This clears any attached dictionary.
     */
    public abstract void reset();

    /**
     * Returns true if this is a high compression (HC) compressor.
     */
    public abstract boolean isHighCompression();

    /**
     * Returns the maximum compressed length for an input of size {@code length}.
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
     */
    public final int compress(byte[] src, byte[] dest) {
        return compress(src, 0, src.length, dest, 0, dest.length);
    }

    /**
     * Convenience method for compressing entire arrays.
     */
    public final int compress(ByteBuffer src, ByteBuffer dest) {
        return compress(src, 0, src.remaining(), dest, 0, dest.remaining());
    }

    /**
     * Convenience method that allocates and returns compressed data.
     */
    public final byte[] compress(byte[] src, int srcOff, int srcLen) {
        int maxLen = maxCompressedLength(srcLen);
        byte[] dest = new byte[maxLen];
        int compressedLen = compress(src, srcOff, srcLen, dest, 0, maxLen);
        return Arrays.copyOf(dest, compressedLen);
    }

    /**
     * Convenience method that allocates and returns compressed data.
     */
    public final ByteBuffer compress(ByteBuffer src, int srcOff, int srcLen) {
        int maxLen = maxCompressedLength(srcLen);
        ByteBuffer dest = ByteBuffer.allocateDirect(maxLen);
        int compressedLen = compress(src, srcOff, srcLen, dest, 0, maxLen);
        dest.limit(compressedLen);
        return dest;
    }


    /**
     * Resets the compressor, attaches the given dictionary, and compresses the data.
     *
     * @param dictionary the dictionary to attach, or null to detach
     * @param src source data
     * @param srcOff offset in source
     * @param srcLen length to compress
     * @param dest destination buffer
     * @param destOff offset in destination
     * @param maxDestLen maximum bytes to write
     * @return compressed size
     * @throws LZ4Exception if compression fails
     */
    public abstract int resetAttachDictCompress(LZ4Dictionary dictionary,
                                                byte[] src, int srcOff, int srcLen,
                                                byte[] dest, int destOff, int maxDestLen);

    /**
     * Convenience method that allocates and returns compressed data.
     */
    public final byte[] compress(byte[] src) {
        return compress(src, 0, src.length);
    }

    /**
     * Convenience method that allocates and returns compressed data.
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
     */
    public abstract boolean isClosed();
}

/**
 * JNI-backed fast streaming compressor.
 */
final class LZ4JNIStreamingCompressor extends LZ4StreamingCompressor {

    private volatile long streamPtr;
    private final int acceleration;

    LZ4JNIStreamingCompressor(int acceleration) {
        this.acceleration = acceleration;
        this.streamPtr = LZ4JNI.LZ4_createStream();
        if (this.streamPtr == 0) {
            throw new LZ4Exception("Failed to create stream");
        }
    }

    @Override
    public boolean isHighCompression() {
        return false;
    }

    @Override
    public void attachDictionary(LZ4Dictionary dictionary) {
        checkNotClosed();
        if (dictionary == null) {
            LZ4JNI.LZ4_attach_dictionary(streamPtr, 0);
        } else {
            if (dictionary.isHighCompression()) {
                throw new IllegalArgumentException(
                    "Cannot attach HC dictionary to fast compressor. " +
                    "Use LZ4Dictionary.create() instead of LZ4Dictionary.createHC()");
            }
            LZ4JNI.LZ4_attach_dictionary(streamPtr, dictionary.getStreamPtr());
        }
    }

    @Override
    public void reset() {
        checkNotClosed();
        LZ4JNI.LZ4_resetStream_fast(streamPtr);
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen,
                        byte[] dest, int destOff, int maxDestLen) {
        checkNotClosed();
        checkRange(src, srcOff, srcLen);
        checkRange(dest, destOff, maxDestLen);

        int result = LZ4JNI.LZ4_compress_fast_continue(
            streamPtr, src, null, srcOff, srcLen,
            dest, null, destOff, maxDestLen, acceleration);

        if (result <= 0) {
            throw new LZ4Exception("Compression failed");
        }
        return result;
    }

    @Override
    public int compress(ByteBuffer src, int srcOff, int srcLen,
                        ByteBuffer dest, int destOff, int maxDestLen) {
        checkNotClosed();
        ByteBufferUtils.checkRange(src, srcOff, srcLen);
        ByteBufferUtils.checkRange(dest, destOff, maxDestLen);

        int result = LZ4JNI.LZ4_compress_fast_continue(
            streamPtr, null, src, srcOff, srcLen,
            null, dest, destOff, maxDestLen, acceleration);

        if (result <= 0) {
            throw new LZ4Exception("Compression failed");
        }
        return result;
    }

    @Override
    public int resetAttachDictCompress(LZ4Dictionary dictionary,
                                        byte[] src, int srcOff, int srcLen,
                                        byte[] dest, int destOff, int maxDestLen) {
        checkNotClosed();
        checkRange(src, srcOff, srcLen);
        checkRange(dest, destOff, maxDestLen);

        long dictPtr = (dictionary == null) ? 0 : dictionary.getStreamPtr();

        int result = LZ4JNI.LZ4_reset_attachDict_compress(streamPtr, dictPtr, acceleration,
            src, null, srcOff, srcLen,
            dest, null, destOff, maxDestLen);

        if (result <= 0) {
            throw new LZ4Exception("Compression failed");
        }
        return result;
    }

    private void checkNotClosed() {
        if (streamPtr == 0) {
            throw new IllegalStateException("Compressor has been closed");
        }
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
            LZ4JNI.LZ4_freeStream(ptr);
        }
    }

    @Override
    public String toString() {
        return "LZ4StreamingCompressor[fast, acceleration=" + acceleration + 
               ", closed=" + isClosed() + "]";
    }
}

/**
 * JNI-backed HC streaming compressor.
 */
final class LZ4JNIStreamingCompressorHC extends LZ4StreamingCompressor {

    private volatile long streamPtr;
    private final int compressionLevel;

    LZ4JNIStreamingCompressorHC(int compressionLevel) {
        this.compressionLevel = compressionLevel;
        this.streamPtr = LZ4JNI.LZ4_createStreamHC();
        if (this.streamPtr == 0) {
            throw new LZ4Exception("Failed to create HC stream");
        }
        LZ4JNI.LZ4_resetStreamHC_fast(this.streamPtr, compressionLevel);
    }

    @Override
    public boolean isHighCompression() {
        return true;
    }

    @Override
    public void attachDictionary(LZ4Dictionary dictionary) {
        checkNotClosed();
        if (dictionary == null) {
            LZ4JNI.LZ4_attach_HC_dictionary(streamPtr, 0);
        } else {
            if (!dictionary.isHighCompression()) {
                throw new IllegalArgumentException(
                    "Cannot attach fast dictionary to HC compressor. " +
                    "Use LZ4Dictionary.createHC() instead of LZ4Dictionary.create()");
            }
            LZ4JNI.LZ4_attach_HC_dictionary(streamPtr, dictionary.getStreamPtr());
        }
    }

    @Override
    public void reset() {
        checkNotClosed();
        LZ4JNI.LZ4_resetStreamHC_fast(streamPtr, compressionLevel);
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen,
                        byte[] dest, int destOff, int maxDestLen) {
        checkNotClosed();
        checkRange(src, srcOff, srcLen);
        checkRange(dest, destOff, maxDestLen);

        int result = LZ4JNI.LZ4_compress_HC_continue(
            streamPtr, src, null, srcOff, srcLen,
            dest, null, destOff, maxDestLen);

        if (result <= 0) {
            throw new LZ4Exception("Compression failed");
        }
        return result;
    }

    @Override
    public int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dest, int destOff, int maxDestLen) {
        checkNotClosed();
        ByteBufferUtils.checkRange(src, srcOff, srcLen);
        ByteBufferUtils.checkRange(dest, destOff, maxDestLen);

        int result = LZ4JNI.LZ4_compress_HC_continue(
            streamPtr, null, src, srcOff, srcLen,
            null, dest, destOff, maxDestLen);

        if (result <= 0) {
            throw new LZ4Exception("Compression failed");
        }
        return result;
    }

    @Override
    public int resetAttachDictCompress(LZ4Dictionary dictionary,
                                       byte[] src, int srcOff, int srcLen,
                                       byte[] dest, int destOff, int maxDestLen) {
        checkNotClosed();
        checkRange(src, srcOff, srcLen);
        checkRange(dest, destOff, maxDestLen);

        long dictPtr = (dictionary == null) ? 0 : dictionary.getStreamPtr();

        int result = LZ4JNI.LZ4_reset_attachDictHC_compress(streamPtr, dictPtr, compressionLevel,
            src, null, srcOff, srcLen,
            dest, null, destOff, maxDestLen);

        if (result <= 0) {
            throw new LZ4Exception("Compression failed");
        }

        return result;
    }

    private void checkNotClosed() {
        if (streamPtr == 0) {
            throw new IllegalStateException("Compressor has been closed");
        }
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
            LZ4JNI.LZ4_freeStreamHC(ptr);
        }
    }

    @Override
    public String toString() {
        return "LZ4StreamingCompressor[HC, level=" + compressionLevel + 
               ", closed=" + isClosed() + "]";
    }
}

