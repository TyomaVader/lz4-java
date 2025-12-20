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

import net.jpountz.util.Native;


/**
 * JNI bindings to the original C implementation of LZ4.
 */
enum LZ4JNI {
  ;

  static {
    Native.load();
    init();
  }

  static native void init();
  static native int LZ4_compress_limitedOutput(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen);
  static native int LZ4_compressHC(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen, int compressionLevel);
  static native int LZ4_decompress_fast(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, byte[] destArray, ByteBuffer destBuffer, int destOff, int destLen);
  static native int LZ4_decompress_safe(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen);
  static native int LZ4_decompress_safe_usingDict(byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen, byte[] dictArray, ByteBuffer dictBuffer, int dictOff, int dictSize);
  static native int LZ4_compressBound(int len);


  /**
   * Creates a new LZ4 streaming compression context.
   * @return pointer to the allocated LZ4_stream_t (as long), or 0 on failure
   */
  static native long LZ4_createStream();

  /**
   * Frees an LZ4 streaming compression context.
   * @param streamPtr pointer to LZ4_stream_t
   * @return 0 on success
   */
  static native int LZ4_freeStream(long streamPtr);

  /**
   * Loads a dictionary into the stream. The dictionary content must remain
   * available during compression. Only the last 64KB are used.
   * After loading, the stream can be used as a shared read-only dictionary.
   * 
   * @param streamPtr pointer to LZ4_stream_t
   * @param dictArray dictionary data
   * @param dictBuffer dictionary data
   * @param dictOff offset in dictArray
   * @param dictSize size of dictionary
   * @return loaded dictionary size in bytes (max 64KB)
   */
  static native int LZ4_loadDict(long streamPtr, byte[] dictArray, ByteBuffer dictBuffer, int dictOff, int dictSize);

  /**
   * Same as LZ4_loadDict but uses more CPU to reference dictionary content
   * more thoroughly, resulting in better compression ratio. Recommended when
   * the dictionary will be reused across multiple compression sessions.
   * (LZ4 1.10.0+)
   */
  static native int LZ4_loadDictSlow(long streamPtr, byte[] dictArray, ByteBuffer dictBuffer, int dictOff, int dictSize);

  /**
   * Attaches a pre-loaded dictionary stream to a working stream.
   * This enables efficient dictionary reuse without copying.
   * 
   * The dictionary stream must have been prepared with LZ4_loadDict[Slow].
   * The dictionary stream is READ-ONLY and can be safely shared across threads.
   * Pass 0 for dictStreamPtr to detach any existing dictionary.
   * 
   * The dictionary remains attached only for the first compression call,
   * then it is automatically cleared.
   * (LZ4 1.10.0+)
   * 
   * @param workingStreamPtr pointer to working LZ4_stream_t
   * @param dictStreamPtr pointer to dictionary LZ4_stream_t (or 0 to detach)
   */
  static native void LZ4_attach_dictionary(long workingStreamPtr, long dictStreamPtr);

  /**
   * Resets a stream for a new compression session. Much faster than
   * creating a new stream. The stream must have been properly initialized.
   */
  static native void LZ4_resetStream_fast(long streamPtr);

  /**
   * Compresses using the streaming context, using previously compressed
   * data and/or attached dictionary for better compression ratio.
   * 
   * @param streamPtr pointer to LZ4_stream_t
   * @param srcArray source data
   * @param srcBuffer source data
   * @param srcOff offset in source
   * @param srcLen length to compress
   * @param destArray destination buffer
   * @param destBuffer destination buffer
   * @param destOff offset in destination
   * @param maxDestLen maximum bytes to write
   * @param acceleration acceleration factor (1 = default)
   * @return compressed size, or 0 on failure
   */
  static native int LZ4_compress_fast_continue(long streamPtr, byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen,
                                               byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen, int acceleration);

  /**
   * Creates a new LZ4 HC streaming compression context.
   * @return pointer to the allocated LZ4_streamHC_t (as long), or 0 on failure
   */
  static native long LZ4_createStreamHC();

  /**
   * Frees an LZ4 HC streaming compression context.
   */
  static native int LZ4_freeStreamHC(long streamPtr);

  /**
   * Loads a dictionary into the HC stream.
   */
  static native int LZ4_loadDictHC(long streamPtr, byte[] dictArray, ByteBuffer dictBuffer, int dictOff, int dictSize);

  /**
   * Attaches a pre-loaded HC dictionary stream to a working HC stream.
   * The dictionary stream is READ-ONLY and can be safely shared across threads.
   * (LZ4 1.10.0+)
   */
  static native void LZ4_attach_HC_dictionary(long workingStreamPtr, long dictStreamPtr);

  /**
   * Resets an HC stream for a new compression session with given compression level.
   */
  static native void LZ4_resetStreamHC_fast(long streamPtr, int compressionLevel);

  /**
   * Compresses using the HC streaming context.
   */
  static native int LZ4_compress_HC_continue(long streamPtr, byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen,
                                             byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen);
}

