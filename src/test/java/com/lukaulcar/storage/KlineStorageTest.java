package com.lukaulcar.storage;

import com.lukaulcar.model.Kline;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class KlineStorageTest {

    @TempDir
    Path tempDir;

    private KlineStorage storage;

    @BeforeEach
    void setUp() throws IOException {
        storage = new KlineStorage(tempDir);
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    @Test
    void testMetadataLifecycle() throws IOException {
        long baseTime = 1700000000000L;
        Metadata meta = new Metadata(baseTime, 123456L, 60_000L, "BTCUSDT");
        storage.writeMetadata("BTCUSDT", "1m", meta);

        Metadata read = storage.readMetadata("BTCUSDT", "1m");
        assertEquals(baseTime, read.firstKlineTime());
        assertEquals(123456L, read.lastApiFetchTime());
        assertEquals(60_000L, read.intervalMillis());
        assertEquals("BTCUSDT", read.coin());

        storage.updateLastApiFetchTime("BTCUSDT", "1m", 999999L);
        Metadata updated = storage.readMetadata("BTCUSDT", "1m");
        assertEquals(baseTime, updated.firstKlineTime());
        assertEquals(999999L, updated.lastApiFetchTime());
        assertEquals("BTCUSDT", updated.coin());
    }

    @Test
    void testDirectOffsetAndGapHandling() throws IOException {
        long baseTime = 1700000000000L;
        Kline k0 = new Kline(100.0, 105.0, 99.0, 102.0, 50.0, baseTime);
        Kline k5 = new Kline(110.0, 115.0, 109.0, 112.0, 60.0, baseTime + (5 * 60_000L));

        storage.writeKlines("ETHUSDT", "1m", List.of(k0, k5));

        Optional<Kline> read0 = storage.getKline("ETHUSDT", "1m", baseTime);
        Optional<Kline> read5 = storage.getKline("ETHUSDT", "1m", baseTime + (5 * 60_000L));
        Optional<Kline> readGap = storage.getKline("ETHUSDT", "1m", baseTime + (2 * 60_000L));

        assertTrue(read0.isPresent());
        assertTrue(read5.isPresent());
        assertTrue(readGap.isEmpty());

        assertEquals(102.0, read0.get().close(), 0.0001);
        assertEquals(112.0, read5.get().close(), 0.0001);

        List<Kline> range = storage.getKlines("ETHUSDT", "1m", baseTime, baseTime + (6 * 60_000L), 0);
        assertEquals(2, range.size());
    }

    @Test
    void testBatchWriteAndRangeScan() throws IOException {
        long baseTime = 1700000000000L;
        int count = 500;
        List<Kline> klines = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            klines.add(new Kline(10, 12, 9, 11, 100 + i, baseTime + (i * 60_000L)));
        }

        storage.writeKlines("SOLUSDT", "1m", klines);

        List<Kline> read = storage.getKlines("SOLUSDT", "1m", baseTime, baseTime + (count * 60_000L), 0);
        assertEquals(count, read.size());

        List<Kline> slice = storage.getKlines("SOLUSDT", "1m", baseTime + (10 * 60_000L), baseTime + (20 * 60_000L), 0);
        assertEquals(10, slice.size());
        assertEquals(baseTime + (10 * 60_000L), slice.get(0).closeTime());
    }

    @Test
    void testDiskStreamingAndIterator() throws IOException {
        long baseTime = 1700000000000L;
        int count = 1000;
        List<Kline> klines = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            klines.add(new Kline(100 + i, 105 + i, 95 + i, 102 + i, 50, baseTime + (i * 60_000L)));
        }

        storage.writeKlines("BTCUSDT", "1m", klines);

        try (Stream<Kline> stream = storage.streamKlines("BTCUSDT", "1m", baseTime, baseTime + (count * 60_000L))) {
            List<Kline> streamed = stream.toList();
            assertEquals(count, streamed.size());
            assertEquals(102.0, streamed.get(0).close());
            assertEquals(102 + count - 1, streamed.get(count - 1).close());
        }

        try (KlineIterator it = storage.iterateKlines("BTCUSDT", "1m", baseTime + (100 * 60_000L), baseTime + (200 * 60_000L))) {
            int iteratedCount = 0;
            while (it.hasNext()) {
                Kline k = it.next();
                assertNotNull(k);
                iteratedCount++;
            }
            assertEquals(100, iteratedCount);
        }
    }

    @Test
    void testMultiTimeframeSeparation() throws IOException {
        long baseTime = 1700000000000L;
        long oneHourMillis = 3_600_000L;

        Kline k1m = new Kline(100, 105, 95, 102, 10, baseTime + 60_000L);
        Kline k1h = new Kline(200, 210, 190, 205, 50, baseTime + oneHourMillis);

        storage.writeKlines("BTCUSDT", "1m", List.of(k1m));
        storage.writeKlines("BTCUSDT", "1h", List.of(k1h));

        Path btcFolder = tempDir.resolve("BTCUSDT");
        assertTrue(java.nio.file.Files.exists(btcFolder.resolve("BTCUSDT_1m.bin")));
        assertTrue(java.nio.file.Files.exists(btcFolder.resolve("BTCUSDT_1h.bin")));

        Optional<Kline> read1m = storage.getLastKline("BTCUSDT", "1m");
        Optional<Kline> read1h = storage.getLastKline("BTCUSDT", "1h");

        assertTrue(read1m.isPresent());
        assertTrue(read1h.isPresent());
        assertEquals(102.0, read1m.get().close());
        assertEquals(205.0, read1h.get().close());

        Metadata meta1m = storage.readMetadata("BTCUSDT", "1m");
        Metadata meta1h = storage.readMetadata("BTCUSDT", "1h");
        assertEquals(60_000L, meta1m.intervalMillis());
        assertEquals(oneHourMillis, meta1h.intervalMillis());
    }

    @Test
    void testDiscoveryAndConvenienceMethods() throws IOException {
        long baseTime = 1700000000000L;
        Kline k1 = new Kline(100, 105, 95, 102, 10, baseTime);
        storage.writeKlines("BTCUSDT", "1m", List.of(k1));
        storage.writeKlines("BTCUSDT", "5m", List.of(k1));
        storage.writeKlines("ETHUSDT", "1h", List.of(k1));

        assertTrue(storage.hasData("BTCUSDT", "1m"));
        assertTrue(storage.hasData("BTCUSDT", "5m"));
        assertTrue(storage.hasData("ETHUSDT", "1h"));
        assertFalse(storage.hasData("DOGEUSDT", "1m"));

        assertEquals(java.util.Set.of("BTCUSDT", "ETHUSDT"), storage.getStoredSymbols());
        assertEquals(List.of("1m", "5m"), storage.getStoredIntervals("BTCUSDT"));
        assertEquals(List.of("1h"), storage.getStoredIntervals("ETHUSDT"));

        List<Kline> allBtc = storage.getAllKlines("BTCUSDT", "1m");
        assertEquals(1, allBtc.size());
        assertEquals(102.0, allBtc.get(0).close());

        try (Stream<Kline> stream = storage.streamAllKlines("BTCUSDT", "1m")) {
            assertEquals(1, stream.count());
        }

        assertTrue(storage.deleteData("BTCUSDT", "5m"));
        assertFalse(storage.hasData("BTCUSDT", "5m"));
        assertEquals(List.of("1m"), storage.getStoredIntervals("BTCUSDT"));
    }
}
