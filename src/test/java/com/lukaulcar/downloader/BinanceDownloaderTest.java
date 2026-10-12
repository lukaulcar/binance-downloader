package com.lukaulcar.downloader;

import com.lukaulcar.model.Kline;
import com.lukaulcar.storage.KlineStorage;
import com.lukaulcar.storage.Metadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class BinanceDownloaderTest {

    @TempDir
    Path tempDir;

    private KlineStorage storage;
    private BinanceDownloader downloader;

    @BeforeEach
    void setUp() throws IOException {
        storage = new KlineStorage(tempDir);
        downloader = new BinanceDownloader(storage);
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    @Test
    void testNormalizeTimestamp() {
        long msTs = 1502942459999L;
        assertEquals(1502942460000L, BinanceDownloader.normalizeTimestamp(msTs));

        long microTs = 1791417659999999L;
        assertEquals(1791417660000L, BinanceDownloader.normalizeTimestamp(microTs));

        long exact = 1704067200000L;
        assertEquals(1704067200000L, BinanceDownloader.normalizeTimestamp(exact));
    }

    @Test
    void testParseCsvLine() {
        String validLine = "1704067200000,42000.5,42100.0,41950.0,42050.0,15.5,1704067259999,650000,120,5.0,210000,0";
        Kline kline = BinanceDownloader.parseCsvLine(validLine, "1m");
        assertNotNull(kline);
        assertEquals(42000.5, kline.open());
        assertEquals(42100.0, kline.high());
        assertEquals(41950.0, kline.low());
        assertEquals(42050.0, kline.close());
        assertEquals(15.5, kline.volume());
        assertEquals(1704067260000L, kline.closeTime());

        String header = "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore";
        assertNull(BinanceDownloader.parseCsvLine(header, "1m"));

        assertNull(BinanceDownloader.parseCsvLine("", "1m"));
        assertNull(BinanceDownloader.parseCsvLine("123,456", "1m"));
    }

    @Test
    void testParseSymbolsFromExchangeInfo() {
        String json = """
                {
                  "timezone": "UTC",
                  "serverTime": 1700000000000,
                  "symbols": [
                    {"symbol": "ETHBTC", "status": "TRADING", "baseAsset": "ETH", "quoteAsset": "BTC"},
                    {"symbol": "BTCUSDT", "status": "TRADING", "baseAsset": "BTC", "quoteAsset": "USDT"},
                    {"symbol": "OLDCOIN", "status": "BREAK", "baseAsset": "OLD", "quoteAsset": "USDT"}
                  ]
                }
                """;

        List<String> tradingOnly = BinanceDownloader.parseSymbolsFromExchangeInfo(json, true);
        assertEquals(List.of("BTCUSDT", "ETHBTC"), tradingOnly);

        List<String> allSymbols = BinanceDownloader.parseSymbolsFromExchangeInfo(json, false);
        assertEquals(List.of("BTCUSDT", "ETHBTC", "OLDCOIN"), allSymbols);
    }

    @Test
    void testProcessZipBytesInMemory() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            zos.putNextEntry(new ZipEntry("TEST-1m-2024-01-01.csv"));

            String csvContent = """
                    open_time,open,high,low,close,volume,close_time
                    1704067320000,102,103,101,102.5,50,1704067379999
                    1704067200000,100,101,99,100.5,25,1704067259999
                    1704067260000,101,102,100,101.5,35,1704067319999
                    """;
            zos.write(csvContent.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        byte[] zipBytes = baos.toByteArray();
        List<Kline> parsed = downloader.processZipBytesInMemory(zipBytes, "1m");

        assertEquals(3, parsed.size());
        assertEquals(1704067260000L, parsed.get(0).closeTime());
        assertEquals(1704067320000L, parsed.get(1).closeTime());
        assertEquals(1704067380000L, parsed.get(2).closeTime());

        assertEquals(100.5, parsed.get(0).close());
        assertEquals(101.5, parsed.get(1).close());
        assertEquals(102.5, parsed.get(2).close());
    }

    @Test
    void testSkipWhenUpdatedWithin24Hours() throws IOException {
        String symbol = "RECENTCOIN";
        long now = System.currentTimeMillis();
        long twoHoursAgo = now - (2 * 3600 * 1000L);

        Metadata meta = new Metadata(twoHoursAgo - 60_000L, twoHoursAgo, 60_000L, symbol);
        storage.writeMetadata(symbol, "1m", meta);

        BinanceDownloader.DownloadResult result = downloader.downloadCoin(symbol, "1m");
        assertEquals(BinanceDownloader.Status.SKIPPED_UP_TO_DATE, result.status());
        assertTrue(result.message().contains("< 24h"));
    }

    @Test
    void testLiveBinanceApiEndpoints() {
        try {
            List<String> symbols = downloader.getAllSymbols();
            assertNotNull(symbols);
            assertTrue(symbols.size() > 500);
            assertTrue(symbols.contains("BTCUSDT"));
            assertTrue(symbols.contains("ETHUSDT"));

            OptionalLong oldestTime = downloader.fetchOldestKlineTime("BTCUSDT", "1m");
            assertTrue(oldestTime.isPresent());
            assertEquals(1502942400000L, oldestTime.getAsLong());
        } catch (Exception e) {
            System.out.println("Live network test skipped: " + e.getMessage());
        }
    }

    @Test
    void testIntervalParsing() {
        assertEquals(60_000L, com.lukaulcar.model.Interval.of("1m").millis());
        assertEquals(300_000L, com.lukaulcar.model.Interval.of("5m").millis());
        assertEquals(900_000L, com.lukaulcar.model.Interval.of("15m").millis());
        assertEquals(3_600_000L, com.lukaulcar.model.Interval.of("1h").millis());
        assertEquals(14_400_000L, com.lukaulcar.model.Interval.of("4h").millis());
        assertEquals(86_400_000L, com.lukaulcar.model.Interval.of("1d").millis());
        assertEquals(604_800_000L, com.lukaulcar.model.Interval.of("1w").millis());
    }

    @Test
    void testParseCsvLineWithCustomInterval() {
        String lineWithoutCloseTime = "1704067200000,100,105,95,102,10";
        long oneHour = 3_600_000L;
        Kline kline = BinanceDownloader.parseCsvLine(lineWithoutCloseTime, oneHour);
        assertNotNull(kline);
        assertEquals(1704067200000L + oneHour, kline.closeTime());
        assertEquals(1704067200000L, kline.openTime(oneHour));
        assertEquals(1704067200000L, kline.openTime(com.lukaulcar.model.Interval.ONE_HOUR));
    }

    @Test
    void testIntervalRobustness() {
        assertFalse(com.lukaulcar.model.Interval.STANDARD_INTERVALS.isEmpty());
        assertTrue(com.lukaulcar.model.Interval.STANDARD_INTERVALS.contains(com.lukaulcar.model.Interval.ONE_MINUTE));

        assertEquals(600_000L, com.lukaulcar.model.Interval.of("10m").millis());

        assertThrows(IllegalArgumentException.class, () -> com.lukaulcar.model.Interval.of("invalid"));
        assertThrows(IllegalArgumentException.class, () -> com.lukaulcar.model.Interval.of("m"));
        assertThrows(IllegalArgumentException.class, () -> com.lukaulcar.model.Interval.of("-5m"));
    }

    @Test
    void testProgressListener() throws IOException {
        java.util.List<String> messages = new java.util.ArrayList<>();
        BinanceDownloader listenerDownloader = new BinanceDownloader(storage, messages::add);

        String symbol = "PROGRESSCOIN";
        long now = System.currentTimeMillis();
        Metadata meta = new Metadata(now - 120_000L, now - 60_000L, 60_000L, symbol);
        storage.writeMetadata(symbol, "1m", meta);

        BinanceDownloader.DownloadResult result = listenerDownloader.downloadCoin(symbol, "1m");
        assertEquals(BinanceDownloader.Status.SKIPPED_UP_TO_DATE, result.status());
    }
}
