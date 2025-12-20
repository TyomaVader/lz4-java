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

#include "lz4.h"
#include "lz4hc.h"
#include <jni.h>

static jclass OutOfMemoryError;

/*
 * Class:     net_jpountz_lz4_LZ4
 * Method:    init
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_net_jpountz_lz4_LZ4JNI_init
  (JNIEnv *env, jclass cls) {
  OutOfMemoryError = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
}

static void throw_OOM(JNIEnv *env) {
  (*env)->ThrowNew(env, OutOfMemoryError, "Out of memory");
}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compress_limitedOutput
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 *
 * Though LZ4_compress_limitedOutput is no longer called as it was deprecated,
 * keep the method name of LZ4_compress_limitedOutput for backward compatibility,
 * so that the old JNI bindings in src/resources can still be used.
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compress_1limitedOutput
  (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen, jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen) {

  char* in;
  char* out;
  jint compressed;

  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }

  if (in == NULL) {
    throw_OOM(env);
    return 0;
  }

  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }

  if (out == NULL) {
    if (srcArray != NULL) {
      (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
    }
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_compress_default(in + srcOff, out + destOff, srcLen, maxDestLen);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compressHC
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;III)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compressHC
  (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen, jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen, jint compressionLevel) {

  char* in;
  char* out;
  jint compressed;
  
  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }

  if (in == NULL) {
    throw_OOM(env);
    return 0;
  } 

  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }

  if (out == NULL) {
    if (srcArray != NULL) {
      (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
    }
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_compress_HC(in + srcOff, out + destOff, srcLen, maxDestLen, compressionLevel);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_decompress_fast
 * Signature: ([BLjava/nio/ByteBuffer;I[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1decompress_1fast
  (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jbyteArray destArray, jobject destBuffer, jint destOff, jint destLen) {

  char* in;
  char* out;
  jint compressed;
  
  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  } 
  
  if (in == NULL) {
    throw_OOM(env);
    return 0;
  } 
    
  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  } 
  
  if (out == NULL) {
    if (srcArray != NULL) {
      (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
    }
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_decompress_fast(in + srcOff, out + destOff, destLen);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_decompress_safe
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1decompress_1safe
  (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen, jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen) {

  char* in;
  char* out;
  jint decompressed;

  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  } 
  
  if (in == NULL) {
    throw_OOM(env);
    return 0;
  } 
    
  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  } 
  
  if (out == NULL) {
    if (srcArray != NULL) {
      (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
    }
    throw_OOM(env);
    return 0;
  }

  decompressed = LZ4_decompress_safe(in + srcOff, out + destOff, srcLen, maxDestLen);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return decompressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_decompress_safe_usingDict
 * Signature: ([BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II[BLjava/nio/ByteBuffer;II)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1decompress_1safe_1usingDict
  (JNIEnv *env, jclass cls, jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen,
   jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen,
   jbyteArray dictArray, jobject dictBuffer, jint dictOff, jint dictSize) {

  char* in;
  char* out;
  char* dict;
  jint decompressed;

  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  } 
  
  if (in == NULL) {
    throw_OOM(env);
    return 0;
  } 
    
  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  } 
  
  if (out == NULL) {
    if (srcArray != NULL) {
      (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
    }
    throw_OOM(env);
    return 0;
  }

  if (dictArray != NULL) {
    dict = (char*) (*env)->GetPrimitiveArrayCritical(env, dictArray, 0);
  } else if (dictBuffer != NULL) {
    dict = (char*) (*env)->GetDirectBufferAddress(env, dictBuffer);
  } else {
    dict = NULL;
  }

  decompressed = LZ4_decompress_safe_usingDict(
      in + srcOff, out + destOff, srcLen, maxDestLen,
      dict != NULL ? dict + dictOff : NULL, dictSize);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }
  if (dictArray != NULL && dict != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, dictArray, dict, 0);
  }

  return decompressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compressBound
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compressBound
  (JNIEnv *env, jclass cls, jint len) {

  return LZ4_compressBound(len);

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_createStream
 * Signature: ()J
 * 
 * Creates a new LZ4 streaming compression context.
 * Returns a pointer (as long) to the allocated LZ4_stream_t.
 */
JNIEXPORT jlong JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1createStream
  (JNIEnv *env, jclass cls) {

  LZ4_stream_t* stream = LZ4_createStream();
  return (jlong)(intptr_t)stream;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_freeStream
 * Signature: (J)I
 * 
 * Frees an LZ4 streaming compression context.
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1freeStream
  (JNIEnv *env, jclass cls, jlong streamPtr) {

  LZ4_stream_t* stream = (LZ4_stream_t*)(intptr_t)streamPtr;
  return LZ4_freeStream(stream);

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_loadDict
 * Signature: (J[BII)I
 * 
 * Loads a dictionary into the stream. The dictionary must remain
 * available during compression. Only the last 64KB are used.
 * After loading, the stream can be used as a shared dictionary.
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1loadDict
  (JNIEnv *env, jclass cls, jlong streamPtr, jbyteArray dictArray, jobject dictBuffer, jint dictOff, jint dictSize) {

  LZ4_stream_t* stream = (LZ4_stream_t*)(intptr_t)streamPtr;
  char* dict;
  if (dictArray != NULL) {
    dict = (char*) (*env)->GetPrimitiveArrayCritical(env, dictArray, 0);
  } else {
    dict = (char*) (*env)->GetDirectBufferAddress(env, dictBuffer);
  }

  if (dict == NULL) {
    throw_OOM(env);
    return 0;
  }

  jint result = LZ4_loadDict(stream, dict + dictOff, dictSize);

  if (dictArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, dictArray, dict, 0);
  }
  
  return result;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_loadDictSlow
 * Signature: (J[BII)I
 * 
 * Same as LZ4_loadDict but uses more CPU to reference dictionary
 * content more thoroughly. Better compression ratio when dictionary
 * is reused across multiple sessions. (LZ4 1.10.0+)
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1loadDictSlow
  (JNIEnv *env, jclass cls, jlong streamPtr, jbyteArray dictArray, jobject dictBuffer, jint dictOff, jint dictSize) {

  LZ4_stream_t* stream = (LZ4_stream_t*)(intptr_t)streamPtr;
  char* dict;
  if (dictArray != NULL) {
    dict = (char*) (*env)->GetPrimitiveArrayCritical(env, dictArray, 0);
  } else {
    dict = (char*) (*env)->GetDirectBufferAddress(env, dictBuffer);
  }

  if (dict == NULL) {
    throw_OOM(env);
    return 0;
  }

  jint result = LZ4_loadDictSlow(stream, dict + dictOff, dictSize);

  if (dictArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, dictArray, dict, 0);
  }
  return result;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_attach_dictionary
 * Signature: (JJ)V
 * 
 * Attaches a pre-loaded dictionary stream to a working stream.
 * The dictionary stream must have been prepared with LZ4_loadDict[Slow].
 * The dictionary stream is READ-ONLY and can be safely shared across threads.
 * Pass 0 for dictStreamPtr to detach any existing dictionary.
 * (LZ4 1.10.0+)
 */
JNIEXPORT void JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1attach_1dictionary
  (JNIEnv *env, jclass cls, jlong workingStreamPtr, jlong dictStreamPtr) {

  LZ4_stream_t* workingStream = (LZ4_stream_t*)(intptr_t)workingStreamPtr;
  const LZ4_stream_t* dictStream = (const LZ4_stream_t*)(intptr_t)dictStreamPtr;
  
  LZ4_attach_dictionary(workingStream, dictStream);

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_resetStream_fast
 * Signature: (J)V
 * 
 * Resets a stream for a new compression session. Much faster than
 * creating a new stream. The stream must have been properly initialized.
 */
JNIEXPORT void JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1resetStream_1fast
  (JNIEnv *env, jclass cls, jlong streamPtr) {

  LZ4_stream_t* stream = (LZ4_stream_t*)(intptr_t)streamPtr;
  LZ4_resetStream_fast(stream);

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compress_fast_continue
 * Signature: (J[BII[BIII)I
 * 
 * Compresses using the streaming context, using previously compressed
 * data and/or attached dictionary for better compression ratio.
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compress_1fast_1continue
  (JNIEnv *env, jclass cls, jlong streamPtr,
  jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen,
  jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen, jint acceleration) {

  LZ4_stream_t* stream = (LZ4_stream_t*)(intptr_t)streamPtr;
  char* in;
  char* out;
  jint compressed;

  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }

  if (in == NULL) {
    throw_OOM(env);
    return 0;
  }

  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }

  if (out == NULL) {
    if (srcArray != NULL) {
      (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
    }
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_compress_fast_continue(stream, in + srcOff, out + destOff, srcLen, maxDestLen, acceleration);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_createStreamHC
 * Signature: ()J
 * 
 * Creates a new LZ4 HC streaming compression context.
 */
JNIEXPORT jlong JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1createStreamHC
  (JNIEnv *env, jclass cls) {

  LZ4_streamHC_t* stream = LZ4_createStreamHC();
  return (jlong)(intptr_t)stream;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_freeStreamHC
 * Signature: (J)I
 * 
 * Frees an LZ4 HC streaming compression context.
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1freeStreamHC
  (JNIEnv *env, jclass cls, jlong streamPtr) {

  LZ4_streamHC_t* stream = (LZ4_streamHC_t*)(intptr_t)streamPtr;
  return LZ4_freeStreamHC(stream);

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_loadDictHC
 * Signature: (J[BII)I
 * 
 * Loads a dictionary into the HC stream.
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1loadDictHC
  (JNIEnv *env, jclass cls, jlong streamPtr, jbyteArray dictArray, jobject dictBuffer, jint dictOff, jint dictSize) {

  LZ4_streamHC_t* stream = (LZ4_streamHC_t*)(intptr_t)streamPtr;
  char* dict;
  if (dictArray != NULL) {
    dict = (char*) (*env)->GetPrimitiveArrayCritical(env, dictArray, 0);
  } else {
    dict = (char*) (*env)->GetDirectBufferAddress(env, dictBuffer);
  }

  if (dict == NULL) {
    throw_OOM(env);
    return 0;
  }

  jint result = LZ4_loadDictHC(stream, dict + dictOff, dictSize);

  if (dictArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, dictArray, dict, 0);
  }
  return result;

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_attach_HC_dictionary
 * Signature: (JJ)V
 * 
 * Attaches a pre-loaded HC dictionary stream to a working HC stream.
 * The dictionary stream is READ-ONLY and can be safely shared across threads.
 * (LZ4 1.10.0+)
 */
JNIEXPORT void JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1attach_1HC_1dictionary
  (JNIEnv *env, jclass cls, jlong workingStreamPtr, jlong dictStreamPtr) {

  LZ4_streamHC_t* workingStream = (LZ4_streamHC_t*)(intptr_t)workingStreamPtr;
  const LZ4_streamHC_t* dictStream = (const LZ4_streamHC_t*)(intptr_t)dictStreamPtr;
  
  LZ4_attach_HC_dictionary(workingStream, dictStream);

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_resetStreamHC_fast
 * Signature: (JI)V
 * 
 * Resets an HC stream for a new compression session with given compression level.
 */
JNIEXPORT void JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1resetStreamHC_1fast
  (JNIEnv *env, jclass cls, jlong streamPtr, jint compressionLevel) {

  LZ4_streamHC_t* stream = (LZ4_streamHC_t*)(intptr_t)streamPtr;
  LZ4_resetStreamHC_fast(stream, compressionLevel);

}

/*
 * Class:     net_jpountz_lz4_LZ4JNI
 * Method:    LZ4_compress_HC_continue
 * Signature: (J[BII[BII)I
 * 
 * Compresses using the HC streaming context.
 */
JNIEXPORT jint JNICALL Java_net_jpountz_lz4_LZ4JNI_LZ4_1compress_1HC_1continue
  (JNIEnv *env, jclass cls, jlong streamPtr,
  jbyteArray srcArray, jobject srcBuffer, jint srcOff, jint srcLen,
  jbyteArray destArray, jobject destBuffer, jint destOff, jint maxDestLen) {

  LZ4_streamHC_t* stream = (LZ4_streamHC_t*)(intptr_t)streamPtr;
  char* in;
  char* out;
  jint compressed;

  if (srcArray != NULL) {
    in = (char*) (*env)->GetPrimitiveArrayCritical(env, srcArray, 0);
  } else {
    in = (char*) (*env)->GetDirectBufferAddress(env, srcBuffer);
  }

  if (in == NULL) {
    throw_OOM(env);
    return 0;
  }

  if (destArray != NULL) {
    out = (char*) (*env)->GetPrimitiveArrayCritical(env, destArray, 0);
  } else {
    out = (char*) (*env)->GetDirectBufferAddress(env, destBuffer);
  }

  if (out == NULL) {
    if (srcArray != NULL) {
      (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
    }
    throw_OOM(env);
    return 0;
  }

  compressed = LZ4_compress_HC_continue(stream, in + srcOff, out + destOff, srcLen, maxDestLen);

  if (srcArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, srcArray, in, 0);
  }
  if (destArray != NULL) {
    (*env)->ReleasePrimitiveArrayCritical(env, destArray, out, 0);
  }

  return compressed;

}
