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

import org.junit.Test;
import static org.junit.Assert.*;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Repeat;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for the streaming dictionary compression API, specifically designed
 * for thread-safe parallel compression with shared dictionaries.
 */
public class LZ4StreamingDictionaryTest extends RandomizedTest {

    private static final LZ4Factory FACTORY = LZ4Factory.nativeInstance();
    private static final LZ4SafeDecompressor DECOMPRESSOR = FACTORY.safeDecompressor();

    /**
     * Creates test data with some repetitive patterns that compress well.
     */
    private static byte[] createTestData(int size) {
        byte[] data = new byte[size];
        // Create some repetitive patterns for better compression
        for (int i = 0; i < size; i++) {
            data[i] = (byte) ((i % 256) ^ (i / 256));
        }
        // Add some random regions
        int randomStart = Math.min(size / 4, 100);
        int randomLen = Math.min(size / 4, 100);
        for (int i = randomStart; i < randomStart + randomLen && i < size; i++) {
            data[i] = (byte) randomInt(255);
        }
        return data;
    }

    /**
     * Creates dictionary data that overlaps with test data patterns.
     */
    private static byte[] createDictionaryData() {
        byte[] dict = new byte[4096];
        // Use patterns similar to test data for better compression
        for (int i = 0; i < dict.length; i++) {
            dict[i] = (byte) ((i % 256) ^ (i / 256));
        }
        return dict;
    }

    @Test
    public void testBasicStreamingCompression() {
        byte[] data = createTestData(1000);
        
        try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            byte[] compressed = compressor.compress(data);
            byte[] decompressed = new byte[data.length];
            
            DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                    decompressed, 0, data.length);
            
            assertArrayEquals(data, decompressed);
        }
    }

    @Test
    public void testBasicHCStreamingCompression() {
        byte[] data = createTestData(1000);
        
        try (LZ4JNIHCStreamingCompressor compressor = FACTORY.highStreamingCompressor()) {
            byte[] compressed = compressor.compress(data);
            byte[] decompressed = new byte[data.length];
            
            DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                    decompressed, 0, data.length);
            
            assertArrayEquals(data, decompressed);
        }
    }

    @Test
    public void testStreamingCompressionWithOffsets() {
        byte[] data = createTestData(1000);
        int srcOff = 100;
        int srcLen = 500;
        
        try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            int maxLen = LZ4StreamingCompressor.maxCompressedLength(srcLen);
            byte[] dest = new byte[maxLen + 50]; // extra space for offset
            int destOff = 25;
            
            int compressedLen = compressor.compress(data, srcOff, srcLen, 
                                                    dest, destOff, maxLen);
            
            assertTrue(compressedLen > 0);
            
            byte[] decompressed = new byte[srcLen];
            DECOMPRESSOR.decompress(dest, destOff, compressedLen, 
                                    decompressed, 0, srcLen);
            
            byte[] expected = Arrays.copyOfRange(data, srcOff, srcOff + srcLen);
            assertArrayEquals(expected, decompressed);
        }
    }

    @Test
    public void testDictionaryCreation() {
        // Test fast dictionary
        try (LZ4JNIFastDictionary dict = FACTORY.fastDictionary()) {
            assertFalse(dict.isClosed());
        }
        
        // Test HC dictionary
        try (LZ4JNIHCDictionary dict = FACTORY.highDictionary()) {
            assertFalse(dict.isClosed());
        }
    }

    @Test
    public void testDictionaryWithOffsets() {
        byte[] fullDict = new byte[8192];
        for (int i = 0; i < fullDict.length; i++) {
            fullDict[i] = (byte) i;
        }
        
        try (LZ4JNIFastDictionary dict = FACTORY.fastDictionary(4096)) {
            dict.load(fullDict, 1000, 4096, false);
            assertFalse(dict.isClosed());
        }
    }

    @Test
    public void testCompressionWithDictionary() {
        byte[] dictData = createDictionaryData();
        byte[] data = createTestData(500);
        
        try (LZ4JNIFastDictionary dict = FACTORY.fastDictionary();
             LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            dict.load(dictData, 0, dictData.length, false);

            compressor.attachDictionary(dict);
            byte[] compressed = compressor.compress(data);
            
            // Decompress using dictionary (required for dictionary-compressed data)
            byte[] decompressed = new byte[data.length];
            DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                    decompressed, 0, data.length,
                                    dictData, 0, dictData.length);
            
            assertArrayEquals(data, decompressed);
        }
    }

    @Test
    public void testHCCompressionWithDictionary() {
        byte[] dictData = createDictionaryData();
        byte[] data = createTestData(500);
        
        try (LZ4JNIHCDictionary dict = FACTORY.highDictionary();
             LZ4JNIHCStreamingCompressor compressor = FACTORY.highStreamingCompressor()) {
            dict.load(dictData, 0, dictData.length);
            
            compressor.attachDictionary(dict);
            byte[] compressed = compressor.compress(data);
            
            // Decompress using dictionary (required for dictionary-compressed data)
            byte[] decompressed = new byte[data.length];
            DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                    decompressed, 0, data.length,
                                    dictData, 0, dictData.length);
            
            assertArrayEquals(data, decompressed);
        }
    }

    @Test
    public void testDictionaryImprovesCompressionRatio() {
        // Create dictionary with specific patterns
        byte[] dictData = new byte[4096];
        String pattern = "The quick brown fox jumps over the lazy dog. ";
        byte[] patternBytes = pattern.getBytes();
        for (int i = 0; i < dictData.length; i++) {
            dictData[i] = patternBytes[i % patternBytes.length];
        }
        
        // Create data using similar patterns
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            sb.append("The quick brown fox jumps over the lazy dog. ");
            sb.append("Lorem ipsum dolor sit amet. ");
        }
        byte[] data = sb.toString().getBytes();
        
        // Compress without dictionary
        byte[] compressedNoDict;
        try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            compressedNoDict = compressor.compress(data);
        }
        
        // Compress with dictionary
        byte[] compressedWithDict;
        try (LZ4JNIFastDictionary dict = FACTORY.fastDictionary();
             LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            dict.load(dictData, 0, dictData.length, true);
            compressor.attachDictionary(dict);
            compressedWithDict = compressor.compress(data);
        }
        
        // Dictionary should help compression (or at worst be equal)
        assertTrue("Dictionary compression should be at least as good: " +
                   "withDict=" + compressedWithDict.length + ", noDict=" + compressedNoDict.length,
                   compressedWithDict.length <= compressedNoDict.length);
        
        // Verify both decompress correctly
        byte[] decompressed = new byte[data.length];
        DECOMPRESSOR.decompress(compressedNoDict, 0, compressedNoDict.length, 
                                decompressed, 0, data.length);
        assertArrayEquals(data, decompressed);
        
        // Dictionary-compressed data requires dictionary for decompression
        DECOMPRESSOR.decompress(compressedWithDict, 0, compressedWithDict.length, 
                                decompressed, 0, data.length,
                                dictData, 0, dictData.length);
        assertArrayEquals(data, decompressed);
    }

    @Test
    public void testMultithreadedCompressionWithSharedDictionary() throws Exception {
        final int NUM_THREADS = 8;
        final int ITERATIONS_PER_THREAD = 100;
        final byte[] dictData = createDictionaryData();
        
        // Create a shared dictionary
        try (LZ4JNIFastDictionary sharedDict = FACTORY.fastDictionary()) {
            sharedDict.load(dictData, 0, dictData.length, true);
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
            try {
                CountDownLatch startLatch = new CountDownLatch(1);
                AtomicInteger successCount = new AtomicInteger(0);
                List<Future<?>> futures = new ArrayList<>();
                
                for (int t = 0; t < NUM_THREADS; t++) {
                    final int threadId = t;
                    futures.add(executor.submit(() -> {
                        try {
                            startLatch.await(); // Wait for all threads to be ready
                            
                            // Each thread has its own compressor
                            try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
                                for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                                    // Create unique data per iteration
                                    byte[] data = createTestData(500 + threadId * 10 + i);
                                    
                                    // Attach shared dictionary and compress
                                    compressor.attachDictionary(sharedDict);
                                    byte[] compressed = compressor.compress(data);
                                    
                                    // Verify decompression using dictionary
                                    byte[] decompressed = new byte[data.length];
                                    DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                                            decompressed, 0, data.length,
                                                            dictData, 0, dictData.length);
                                    
                                    if (Arrays.equals(data, decompressed)) {
                                        successCount.incrementAndGet();
                                    }
                                    
                                    // Reset for next iteration
                                    compressor.reset();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }));
                }
                
                // Start all threads simultaneously
                startLatch.countDown();
                
                // Wait for completion
                for (Future<?> future : futures) {
                    future.get(60, TimeUnit.SECONDS);
                }
                
                assertEquals(NUM_THREADS * ITERATIONS_PER_THREAD, successCount.get());
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testMultithreadedHCCompressionWithSharedDictionary() throws Exception {
        final int NUM_THREADS = 4;
        final int ITERATIONS_PER_THREAD = 50;
        final byte[] dictData = createDictionaryData();
        
        try (LZ4JNIHCDictionary sharedDict = FACTORY.highDictionary()) {
            sharedDict.load(dictData, 0, dictData.length);
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
            try {
                CountDownLatch startLatch = new CountDownLatch(1);
                AtomicInteger successCount = new AtomicInteger(0);
                List<Future<?>> futures = new ArrayList<>();
                
                for (int t = 0; t < NUM_THREADS; t++) {
                    final int threadId = t;
                    futures.add(executor.submit(() -> {
                        try {
                            startLatch.await();
                            
                            try (LZ4JNIHCStreamingCompressor compressor = FACTORY.highStreamingCompressor(9)) {
                                for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                                    byte[] data = createTestData(300 + threadId * 10 + i);
                                    
                                    compressor.attachDictionary(sharedDict);
                                    byte[] compressed = compressor.compress(data);
                                    
                                    // Decompress using dictionary (required for dictionary-compressed data)
                                    byte[] decompressed = new byte[data.length];
                                    DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                                            decompressed, 0, data.length,
                                                            dictData, 0, dictData.length);
                                    
                                    if (Arrays.equals(data, decompressed)) {
                                        successCount.incrementAndGet();
                                    }
                                    
                                    compressor.reset();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(e);
                        }
                    }));
                }
                
                startLatch.countDown();
                
                for (Future<?> future : futures) {
                    future.get(120, TimeUnit.SECONDS);
                }
                
                assertEquals(NUM_THREADS * ITERATIONS_PER_THREAD, successCount.get());
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testConcurrentDictionaryAccess() throws Exception {
        final int NUM_THREADS = 16;
        final byte[] dictData = createDictionaryData();
        
        try (LZ4JNIFastDictionary sharedDict = FACTORY.fastDictionary()) {
            sharedDict.load(dictData, 0, dictData.length, false);
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
            try {
                CountDownLatch latch = new CountDownLatch(NUM_THREADS);
                AtomicInteger errors = new AtomicInteger(0);
                
                for (int t = 0; t < NUM_THREADS; t++) {
                    final int threadId = t;
                    executor.submit(() -> {
                        try {
                            // Tight loop accessing the shared dictionary
                            try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
                                for (int i = 0; i < 1000; i++) {
                                    compressor.attachDictionary(sharedDict);
                                    compressor.attachDictionary(null); // detach
                                    compressor.attachDictionary(sharedDict);
                                    
                                    byte[] data = new byte[100 + threadId];
                                    Arrays.fill(data, (byte) threadId);
                                    byte[] compressed = compressor.compress(data);
                                    
                                    // Verify compression produced output
                                    if (compressed.length == 0) {
                                        throw new AssertionError("Compression produced empty output");
                                    }
                                    
                                    compressor.reset();
                                }
                            }
                        } catch (Exception e) {
                            errors.incrementAndGet();
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                
                assertTrue(latch.await(60, TimeUnit.SECONDS));
                assertEquals(0, errors.get());
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testUseClosedCompressor() {
        LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor();
        compressor.close();
        
        // Should throw
        compressor.compress(new byte[100]);
    }

    @Test(expected = IllegalStateException.class)
    public void testUseClosedDictionary() {
        byte[] dictData = createDictionaryData();
        LZ4JNIFastDictionary dict = FACTORY.fastDictionary();
        dict.load(dictData, 0, dictData.length, true);
        dict.close();
        
        try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            // Should throw when dictionary is accessed
            compressor.attachDictionary(dict);
        }
    }

    @Test
    public void testCloseIdempotent() {
        byte[] dictData = createDictionaryData();
        
        LZ4JNIFastDictionary dict = FACTORY.fastDictionary();
        dict.load(dictData, 0, dictData.length, true);
        dict.close();
        dict.close(); // Should not throw
        assertTrue(dict.isClosed());
        
        LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor();
        compressor.close();
        compressor.close(); // Should not throw
        assertTrue(compressor.isClosed());
    }

    @Test
    public void testDetachDictionary() {
        byte[] dictData = createDictionaryData();
        byte[] data = createTestData(500);
        
        try (LZ4JNIFastDictionary dict = FACTORY.fastDictionary();
             LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            dict.load(dictData, 0, dictData.length, false);
            
            // Attach dictionary
            compressor.attachDictionary(dict);
            
            // Detach by passing null
            compressor.attachDictionary(null);
            
            // Should still work without dictionary (standard decompression)
            byte[] compressed = compressor.compress(data);
            byte[] decompressed = new byte[data.length];
            DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                    decompressed, 0, data.length);
            assertArrayEquals(data, decompressed);
        }
    }

    @Test
    public void testEmptyInput() {
        try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            byte[] compressed = compressor.compress(new byte[0]);
            assertNotNull(compressed);
            // LZ4 compresses empty input to 1 byte (value 0)
            assertEquals(1, compressed.length);
            assertEquals(0, compressed[0]);
            
            // Verify it can be decompressed
            byte[] decompressed = new byte[0];
            DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                    decompressed, 0, 0);
            assertEquals(0, decompressed.length);
        }
    }

    @Test
    public void testSmallInput() {
        byte[] data = new byte[] {1, 2, 3};
        
        try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            byte[] compressed = compressor.compress(data);
            byte[] decompressed = new byte[data.length];
            DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                    decompressed, 0, data.length);
            assertArrayEquals(data, decompressed);
        }
    }

    @Test
    public void testLargeInput() {
        byte[] data = createTestData(1024 << 10); // 1MB
        
        try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            byte[] compressed = compressor.compress(data);
            byte[] decompressed = new byte[data.length];
            DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                    decompressed, 0, data.length);
            assertArrayEquals(data, decompressed);
        }
    }

    @Test
    public void testResetBetweenCompressions() {
        byte[] data1 = createTestData(500);
        byte[] data2 = createTestData(600);
        
        try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            byte[] compressed1 = compressor.compress(data1);
            compressor.reset();
            byte[] compressed2 = compressor.compress(data2);
            
            byte[] decompressed1 = new byte[data1.length];
            DECOMPRESSOR.decompress(compressed1, 0, compressed1.length, 
                                    decompressed1, 0, data1.length);
            assertArrayEquals(data1, decompressed1);
            
            byte[] decompressed2 = new byte[data2.length];
            DECOMPRESSOR.decompress(compressed2, 0, compressed2.length, 
                                    decompressed2, 0, data2.length);
            assertArrayEquals(data2, decompressed2);
        }
    }

    @Test
    public void testAccelerationLevels() {
        byte[] data = createTestData(10000);
        
        // Test different acceleration levels
        for (int accel : new int[] {1, 5, 10, 50}) {
            try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor(accel)) {
                byte[] compressed = compressor.compress(data);
                byte[] decompressed = new byte[data.length];
                DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                        decompressed, 0, data.length);
                assertArrayEquals("Acceleration " + accel, data, decompressed);
            }
        }
    }

    @Test
    public void testHCCompressionLevels() {
        byte[] data = createTestData(10000);
        
        // Test different compression levels
        for (int level : new int[] {1, 6, 9, 12}) {
            try (LZ4JNIHCStreamingCompressor compressor = FACTORY.highStreamingCompressor(level)) {
                byte[] compressed = compressor.compress(data);
                byte[] decompressed = new byte[data.length];
                DECOMPRESSOR.decompress(compressed, 0, compressed.length, 
                                        decompressed, 0, data.length);
                assertArrayEquals("Level " + level, data, decompressed);
            }
        }
    }

    @Test
    public void testCompressorToString() {
        try (LZ4JNIFastStreamingCompressor fast = FACTORY.fastStreamingCompressor();
             LZ4JNIHCStreamingCompressor hc = FACTORY.highStreamingCompressor()) {
            
            assertTrue(fast.toString().contains("Fast"));
            assertTrue(hc.toString().contains("HC"));
        }
    }

    @Test
    public void testDictionaryToString() {
        byte[] dictData = createDictionaryData();
        
        try (LZ4JNIFastDictionary fast = FACTORY.fastDictionary();
             LZ4JNIHCDictionary hc = FACTORY.highDictionary()) {
            fast.load(dictData, 0, dictData.length, false);
            hc.load(dictData, 0, dictData.length);
            
            assertTrue(fast.toString().contains("Fast"));
            assertTrue(hc.toString().contains("HC"));
        }
    }

    @Test
    public void testParallelBlockCompression() throws Exception {
        final int TOTAL_SIZE = 2 * 1024 * 1024; // 2MB
        final int BLOCK_SIZE = 64 << 10; // 64KB blocks
        final int NUM_THREADS = 4;
        final byte[] dictData = createDictionaryData();
        
        // Create large test data
        byte[] largeData = createTestData(TOTAL_SIZE);
        int numBlocks = (TOTAL_SIZE + BLOCK_SIZE - 1) / BLOCK_SIZE; // Round up
        
        // Create shared dictionary
        try (LZ4JNIFastDictionary sharedDict = FACTORY.fastDictionary()) {
            sharedDict.load(dictData, 0, dictData.length, false);
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
            try {
                // Compress blocks in parallel
                @SuppressWarnings("unchecked")
                Future<byte[]>[] compressedFutures = new Future[numBlocks];
                
                for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
                    final int blockStart = blockIdx * BLOCK_SIZE;
                    final int blockLen = Math.min(BLOCK_SIZE, TOTAL_SIZE - blockStart);
                    
                    compressedFutures[blockIdx] = executor.submit(() -> {
                        try (LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
                            // Attach shared dictionary for better compression
                            compressor.attachDictionary(sharedDict);

                            // Compress this block directly from largeData using offsets (no copy)
                            int maxCompressedLen = LZ4StreamingCompressor.maxCompressedLength(blockLen);
                            byte[] compressed = new byte[maxCompressedLen];
                            int compressedLen = compressor.compress(
                                largeData, blockStart, blockLen,
                                compressed, 0, maxCompressedLen);
                            return Arrays.copyOf(compressed, compressedLen);
                        }
                    });
                }
                
                // Collect all compressed blocks
                List<byte[]> compressedBlocks = new ArrayList<>(numBlocks);
                for (int i = 0; i < numBlocks; i++) {
                    compressedBlocks.add(compressedFutures[i].get(30, TimeUnit.SECONDS));
                }
                
                // Decompress all blocks directly into reconstructed array (no intermediate copy)
                byte[] reconstructed = new byte[TOTAL_SIZE];
                
                @SuppressWarnings("unchecked")
                Future<Integer>[] decompressedFutures = new Future[numBlocks];

                for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
                    final int blockStart = blockIdx * BLOCK_SIZE;
                    final int blockLen = Math.min(BLOCK_SIZE, TOTAL_SIZE - blockStart);
                    final byte[] compressed = compressedBlocks.get(blockIdx);

                    decompressedFutures[blockIdx] = executor.submit(() -> {
                        // Decompress directly into reconstructed array using dictionary
                        return DECOMPRESSOR.decompress(
                            compressed, 0, compressed.length,
                            reconstructed, blockStart, blockLen,
                            dictData, 0, dictData.length);
                    });
                }

                for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
                    int blockStart = blockIdx * BLOCK_SIZE;
                    int blockLen = Math.min(BLOCK_SIZE, TOTAL_SIZE - blockStart);
                    int decompressedLen = decompressedFutures[blockIdx].get(30, TimeUnit.SECONDS);

                    assertEquals("Block " + blockIdx + " decompressed length", blockLen, decompressedLen);
                }
                
                // Verify reconstructed data matches original
                assertArrayEquals("Reconstructed data should match original", largeData, reconstructed);
                
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testParallelBlockCompressionHC() throws Exception {
        final int TOTAL_SIZE = 1024 * 1024; // 1MB
        final int BLOCK_SIZE = 32 * 1024; // 32KB blocks
        final int NUM_THREADS = 4;
        final byte[] dictData = createDictionaryData();
        
        // Create large test data
        byte[] largeData = createTestData(TOTAL_SIZE);
        int numBlocks = (TOTAL_SIZE + BLOCK_SIZE - 1) / BLOCK_SIZE; // Round up
        
        // Create shared HC dictionary
        try (LZ4JNIHCDictionary sharedDict = FACTORY.highDictionary()) {
            sharedDict.load(dictData, 0, dictData.length);
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
            try {
                // Compress blocks in parallel
                @SuppressWarnings("unchecked")
                Future<byte[]>[] compressedFutures = new Future[numBlocks];
                
                for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
                    final int blockStart = blockIdx * BLOCK_SIZE;
                    final int blockLen = Math.min(BLOCK_SIZE, TOTAL_SIZE - blockStart);
                    
                    compressedFutures[blockIdx] = executor.submit(() -> {
                        try (LZ4JNIHCStreamingCompressor compressor = FACTORY.highStreamingCompressor(9)) {
                            // Attach shared dictionary for better compression
                            compressor.attachDictionary(sharedDict);
                            
                            // Compress this block directly from largeData using offsets (no copy)
                            int maxCompressedLen = LZ4StreamingCompressor.maxCompressedLength(blockLen);
                            byte[] compressed = new byte[maxCompressedLen];
                            int compressedLen = compressor.compress(
                                largeData, blockStart, blockLen,
                                compressed, 0, maxCompressedLen);
                            return Arrays.copyOf(compressed, compressedLen);
                        }
                    });
                }
                
                // Collect all compressed blocks
                List<byte[]> compressedBlocks = new ArrayList<>(numBlocks);
                for (int i = 0; i < numBlocks; i++) {
                    byte[] block = compressedFutures[i].get(60, TimeUnit.SECONDS);
                    compressedBlocks.add(block);
                }

                // Decompress all blocks directly into reconstructed array (no intermediate copy)
                byte[] reconstructed = new byte[TOTAL_SIZE];
                
                @SuppressWarnings("unchecked")
                Future<Integer>[] decompressedFutures = new Future[numBlocks];

                for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
                    final int blockStart = blockIdx * BLOCK_SIZE;
                    final int blockLen = Math.min(BLOCK_SIZE, TOTAL_SIZE - blockStart);
                    final byte[] compressed = compressedBlocks.get(blockIdx);

                    decompressedFutures[blockIdx] = executor.submit(() -> {
                        // Decompress directly into reconstructed array using dictionary
                        return DECOMPRESSOR.decompress(
                            compressed, 0, compressed.length,
                            reconstructed, blockStart, blockLen,
                            dictData, 0, dictData.length);
                    });
                }

                for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
                    int blockStart = blockIdx * BLOCK_SIZE;
                    int blockLen = Math.min(BLOCK_SIZE, TOTAL_SIZE - blockStart);
                    int decompressedLen = decompressedFutures[blockIdx].get(30, TimeUnit.SECONDS);

                    assertEquals("Block " + blockIdx + " decompressed length", blockLen, decompressedLen);
                }

                // Verify reconstructed data matches original
                assertArrayEquals("Reconstructed data should match original", largeData, reconstructed);
                
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testResetAttachDictCompress() {
        byte[] dictData = createDictionaryData();
        byte[] data = createTestData(500);
        
        try (LZ4JNIFastDictionary dict = FACTORY.fastDictionary();
             LZ4JNIFastStreamingCompressor compressor = FACTORY.fastStreamingCompressor()) {
            dict.load(dictData, 0, dictData.length, false);
            
            int maxLen = LZ4StreamingCompressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxLen];
            
            // Use the combined reset+attach+compress method
            int compressedLen = compressor.resetAttachDictCompress(dict,
                data, 0, data.length, compressed, 0, maxLen);
            
            assertTrue(compressedLen > 0);
            
            // Verify decompression
            byte[] decompressed = new byte[data.length];
            DECOMPRESSOR.decompress(compressed, 0, compressedLen, 
                                    decompressed, 0, data.length,
                                    dictData, 0, dictData.length);
            assertArrayEquals(data, decompressed);
        }
    }

    @Test
    public void testResetAttachDictCompressHC() {
        byte[] dictData = createDictionaryData();
        byte[] data = createTestData(500);
        
        try (LZ4JNIHCDictionary dict = FACTORY.highDictionary();
             LZ4JNIHCStreamingCompressor compressor = FACTORY.highStreamingCompressor()) {
            dict.load(dictData, 0, dictData.length);
            
            int maxLen = LZ4StreamingCompressor.maxCompressedLength(data.length);
            byte[] compressed = new byte[maxLen];
            
            // Use the combined reset+attach+compress method
            int compressedLen = compressor.resetAttachDictCompress(dict,
                data, 0, data.length, compressed, 0, maxLen);
            
            assertTrue(compressedLen > 0);
            
            // Verify decompression
            byte[] decompressed = new byte[data.length];
            DECOMPRESSOR.decompress(compressed, 0, compressedLen, 
                                    decompressed, 0, data.length,
                                    dictData, 0, dictData.length);
            assertArrayEquals(data, decompressed);
        }
    }
}
