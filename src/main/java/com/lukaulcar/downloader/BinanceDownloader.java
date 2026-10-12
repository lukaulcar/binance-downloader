package com.lukaulcar.downloader;

import com.lukaulcar.model.Interval;
import com.lukaulcar.model.Kline;
import com.lukaulcar.storage.KlineStorage;
import com.lukaulcar.storage.Metadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * High-performance downloader for historical and recent Binance klines (candlesticks).
 * Downloads official monthly and daily archives from Binance Vision, falling back seamlessly
 * between available archive granularities and storing normalized candles directly into {@link KlineStorage}.
 */
public class BinanceDownloader {

    private static final System.Logger LOGGER = System.getLogger(BinanceDownloader.class.getName());

    private static final String BINANCE_API_BASE = "https://api.binance.com";
    private static final String BINANCE_VISION_BASE = "https://data.binance.vision/data/spot";

    private static final DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");
    private static final DateTimeFormatter DAY_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final KlineStorage storage;
    private final HttpClient httpClient;
    private final Duration requestTimeout;
    private final Consumer<String> progressListener;

    /**
     * Download operation result status.
     */
    public enum Status {
        /** Download completed successfully and new klines were saved. */
        SUCCESS,
        /** Data is already up to date with the latest available archive day; skipped. */
        SKIPPED_UP_TO_DATE,
        /** Coin symbol was not found or has no trading history on Binance. */
        NO_DATA,
        /** Download encountered an error. */
        ERROR
    }

    /**
     * Detailed result summary of a coin download operation.
     *
     * @param symbol          coin symbol (e.g. "BTCUSDT")
     * @param interval        candle interval (e.g. "1m", "5m")
     * @param status          final status of the download operation
     * @param filesDownloaded number of ZIP archive files downloaded
     * @param klinesSaved     total count of klines saved to disk
     * @param startKlineTime  earliest candle timestamp in the downloaded dataset
     * @param endKlineTime    latest candle timestamp saved
     * @param duration        total wall-clock elapsed duration
     * @param message         informative description or error message
     */
    public record DownloadResult(
            String symbol,
            String interval,
            Status status,
            int filesDownloaded,
            long klinesSaved,
            long startKlineTime,
            long endKlineTime,
            Duration duration,
            String message
    ) {
        @Override
        public String toString() {
            return String.format(
                    "DownloadResult[symbol=%s, interval=%s, status=%s, files=%d, klines=%d, duration=%dms, msg=%s]",
                    symbol, interval, status, filesDownloaded, klinesSaved, duration.toMillis(), message
            );
        }
    }

    /**
     * Constructs a BinanceDownloader using the default HTTP client and 60-second request timeout.
     *
     * @param storage storage engine where klines will be persisted
     */
    public BinanceDownloader(KlineStorage storage) {
        this(storage, null);
    }

    /**
     * Constructs a BinanceDownloader with an optional progress message listener.
     *
     * @param storage          storage engine where klines will be persisted
     * @param progressListener callback invoked with download progress updates (can be null)
     */
    public BinanceDownloader(KlineStorage storage, Consumer<String> progressListener) {
        this(storage, HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(15))
                .build(), Duration.ofSeconds(60), progressListener);
    }

    /**
     * Constructs a BinanceDownloader with a custom HTTP client and timeout.
     *
     * @param storage        storage engine where klines will be persisted
     * @param httpClient     configured {@link HttpClient}
     * @param requestTimeout timeout for individual HTTP requests
     */
    public BinanceDownloader(KlineStorage storage, HttpClient httpClient, Duration requestTimeout) {
        this(storage, httpClient, requestTimeout, null);
    }

    /**
     * Constructs a BinanceDownloader with full customization.
     *
     * @param storage          storage engine where klines will be persisted
     * @param httpClient       configured {@link HttpClient}
     * @param requestTimeout   timeout for individual HTTP requests
     * @param progressListener callback invoked with download progress updates (can be null)
     */
    public BinanceDownloader(KlineStorage storage, HttpClient httpClient, Duration requestTimeout, Consumer<String> progressListener) {
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient cannot be null");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout cannot be null");
        this.progressListener = progressListener;
    }

    private void logProgress(String message) {
        LOGGER.log(System.Logger.Level.INFO, message);
        if (progressListener != null) {
            progressListener.accept(message);
        }
    }

    /**
     * Fetches all currently trading spot symbols from the Binance API.
     *
     * @return sorted list of symbol names (e.g. ["BTCUSDT", "ETHUSDT", ...])
     * @throws IOException          if network communication fails
     * @throws InterruptedException if interrupted while waiting for the response
     */
    public List<String> getAllSymbols() throws IOException, InterruptedException {
        return getAllSymbols(true);
    }

    /**
     * Fetches all spot symbols from the Binance API, optionally filtering for actively trading pairs.
     *
     * @param tradingOnly if true, only includes symbols with status "TRADING"
     * @return sorted list of symbol names
     * @throws IOException          if network communication fails
     * @throws InterruptedException if interrupted while waiting for the response
     */
    public List<String> getAllSymbols(boolean tradingOnly) throws IOException, InterruptedException {
        String url = BINANCE_API_BASE + "/api/v3/exchangeInfo?permissions=SPOT";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(requestTimeout)
                .header("User-Agent", "Binance-Downloader/1.0")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Failed to fetch exchangeInfo from Binance: HTTP " + response.statusCode());
        }

        return parseSymbolsFromExchangeInfo(response.body(), tradingOnly);
    }

    static List<String> parseSymbolsFromExchangeInfo(String json, boolean tradingOnly) {
        List<String> result = new ArrayList<>();
        int symbolsIdx = json.indexOf("\"symbols\":");
        if (symbolsIdx == -1) {
            return result;
        }

        int current = symbolsIdx;
        while (current < json.length()) {
            int symKeyIdx = json.indexOf("\"symbol\":", current);
            if (symKeyIdx == -1) {
                break;
            }

            int firstQuote = json.indexOf('"', symKeyIdx + 9);
            if (firstQuote == -1) break;
            int secondQuote = json.indexOf('"', firstQuote + 1);
            if (secondQuote == -1) break;
            String symbol = json.substring(firstQuote + 1, secondQuote);

            if (tradingOnly) {
                int nextSym = json.indexOf("\"symbol\":", secondQuote);
                int limit = (nextSym != -1) ? nextSym : json.length();
                int statusKeyIdx = json.indexOf("\"status\":", secondQuote);
                if (statusKeyIdx != -1 && statusKeyIdx < limit) {
                    int sQuote1 = json.indexOf('"', statusKeyIdx + 9);
                    int sQuote2 = (sQuote1 != -1) ? json.indexOf('"', sQuote1 + 1) : -1;
                    if (sQuote1 != -1 && sQuote2 != -1) {
                        String status = json.substring(sQuote1 + 1, sQuote2);
                        if ("TRADING".equalsIgnoreCase(status)) {
                            result.add(symbol);
                        }
                    }
                }
            } else {
                result.add(symbol);
            }

            current = secondQuote + 1;
        }

        Collections.sort(result);
        return result;
    }

    /**
     * Fetches the timestamp of the earliest available candle for the given symbol on Binance.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval candle {@link Interval}
     * @return {@link OptionalLong} containing the earliest open timestamp, or empty if not found
     * @throws IOException          if network communication fails
     * @throws InterruptedException if interrupted while waiting for the response
     */
    public OptionalLong fetchOldestKlineTime(String symbol, Interval interval) throws IOException, InterruptedException {
        return fetchOldestKlineTime(symbol, interval.name());
    }

    /**
     * Fetches the timestamp of the earliest available candle for the given symbol on Binance.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m", "5m")
     * @return {@link OptionalLong} containing the earliest open timestamp, or empty if not found
     * @throws IOException          if network communication fails
     * @throws InterruptedException if interrupted while waiting for the response
     */
    public OptionalLong fetchOldestKlineTime(String symbol, String interval) throws IOException, InterruptedException {
        String cleanSymbol = symbol.trim().toUpperCase();
        String url = String.format("%s/api/v3/klines?symbol=%s&interval=%s&startTime=0&limit=1",
                BINANCE_API_BASE, cleanSymbol, interval);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(requestTimeout)
                .header("User-Agent", "Binance-Downloader/1.0")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            return OptionalLong.empty();
        }

        String body = response.body().trim();
        if (!body.startsWith("[[") || body.length() < 10) {
            return OptionalLong.empty();
        }

        int commaIdx = body.indexOf(',');
        if (commaIdx == -1) {
            return OptionalLong.empty();
        }

        String openTimeStr = body.substring(2, commaIdx).trim();
        try {
            long openTime = Long.parseLong(openTimeStr);
            return OptionalLong.of(openTime);
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }

    /**
     * Downloads and persists historical klines for a single coin.
     *
     * @param coin     trading symbol (e.g. "BTCUSDT")
     * @param interval candle {@link Interval}
     * @return {@link DownloadResult} describing the outcome
     */
    public DownloadResult downloadCoin(String coin, Interval interval) {
        return downloadCoin(coin, interval.name());
    }

    /**
     * Downloads and persists historical klines for a single coin.
     *
     * @param coin        trading symbol (e.g. "BTCUSDT")
     * @param intervalStr interval string (e.g. "1m", "5m")
     * @return {@link DownloadResult} describing the outcome
     */
    public DownloadResult downloadCoin(String coin, String intervalStr) {
        long startWallTime = System.currentTimeMillis();
        String symbol = coin.trim().toUpperCase();
        Interval interval = Interval.of(intervalStr);
        String intervalName = interval.name();
        long intervalMillis = interval.millis();

        try {
            long startTimeMillis;
            boolean isNewCoin = false;

            try {
                Metadata meta = storage.readMetadata(symbol, intervalName);
                long mostRecentTime = meta.mostRecentKlineTime();

                if (mostRecentTime == 0) {
                    mostRecentTime = storage.getLastKline(symbol, intervalName)
                            .map(Kline::closeTime)
                            .orElse(meta.firstKlineTime());
                }

                if (mostRecentTime > 0) {
                    long now = System.currentTimeMillis();
                    long elapsed = now - mostRecentTime;
                    if (elapsed < 24L * 60 * 60 * 1000L) {
                        return new DownloadResult(
                                symbol,
                                intervalName,
                                Status.SKIPPED_UP_TO_DATE,
                                0,
                                0,
                                meta.firstKlineTime(),
                                mostRecentTime,
                                Duration.ofMillis(System.currentTimeMillis() - startWallTime),
                                "Last kline was " + Duration.ofMillis(elapsed).toHours() + "h ago (< 24h). Skipped as Binance uploads daily."
                        );
                    }
                    startTimeMillis = mostRecentTime;
                } else {
                    OptionalLong oldestOpt = fetchOldestKlineTime(symbol, intervalName);
                    if (oldestOpt.isEmpty()) {
                        return new DownloadResult(
                                symbol,
                                intervalName,
                                Status.NO_DATA,
                                0,
                                0,
                                0,
                                0,
                                Duration.ofMillis(System.currentTimeMillis() - startWallTime),
                                "Coin not found or has no klines on Binance API"
                        );
                    }
                    isNewCoin = true;
                    startTimeMillis = oldestOpt.getAsLong();
                }

            } catch (NoSuchFileException e) {
                isNewCoin = true;
                OptionalLong oldestOpt = fetchOldestKlineTime(symbol, intervalName);
                if (oldestOpt.isEmpty()) {
                    return new DownloadResult(
                            symbol,
                            intervalName,
                            Status.NO_DATA,
                            0,
                            0,
                            0,
                            0,
                            Duration.ofMillis(System.currentTimeMillis() - startWallTime),
                            "Coin not found or has no klines on Binance API"
                    );
                }
                startTimeMillis = oldestOpt.getAsLong();
            }

            LocalDate startDate = Instant.ofEpochMilli(startTimeMillis).atZone(ZoneOffset.UTC).toLocalDate();
            LocalDate today = LocalDate.now(ZoneOffset.UTC);
            LocalDate yesterday = today.minusDays(1);

            if (startDate.isAfter(yesterday)) {
                return new DownloadResult(
                        symbol,
                        intervalName,
                        Status.SKIPPED_UP_TO_DATE,
                        0,
                        0,
                        startTimeMillis,
                        startTimeMillis,
                        Duration.ofMillis(System.currentTimeMillis() - startWallTime),
                        "Data is already up to date with the latest available archive day (" + yesterday + ")"
                );
            }

            logProgress(String.format("[%s - %s] Downloading historical archives from %s to %s...", symbol, intervalName, startDate, yesterday));

            int filesDownloaded = 0;
            long totalKlinesSaved = 0;
            long latestRecordedTime = startTimeMillis;

            YearMonth currentMonth = YearMonth.from(startDate);
            YearMonth endMonth = YearMonth.from(yesterday);
            YearMonth currentCalendarMonth = YearMonth.from(today);

            while (!currentMonth.isAfter(endMonth)) {
                boolean isCompletedPastMonth = currentMonth.isBefore(currentCalendarMonth);

                if (isCompletedPastMonth) {
                    String monthlyUrl = String.format(
                            "%s/monthly/klines/%s/%s/%s-%s-%s.zip",
                            BINANCE_VISION_BASE, symbol, intervalName, symbol, intervalName, currentMonth.format(MONTH_FORMATTER)
                    );

                    byte[] zipBytes = downloadZipBytesInMemory(monthlyUrl);
                    if (zipBytes != null) {
                        List<Kline> batch = processZipBytesInMemory(zipBytes, intervalMillis);
                        if (!batch.isEmpty()) {
                            long lastBatchTime = saveKlinesBatch(symbol, intervalName, batch, isNewCoin);
                            isNewCoin = false;
                            filesDownloaded++;
                            totalKlinesSaved += batch.size();
                            latestRecordedTime = Math.max(latestRecordedTime, lastBatchTime);
                            logProgress(String.format("[%s - %s] Saved monthly archive %s: %,d klines (Total: %,d)",
                                    symbol, intervalName, currentMonth, batch.size(), totalKlinesSaved));
                        }
                        currentMonth = currentMonth.plusMonths(1);
                        continue;
                    }
                }

                LocalDate monthStartDay = currentMonth.atDay(1);
                LocalDate dayIter = monthStartDay.isBefore(startDate) ? startDate : monthStartDay;
                LocalDate monthEndDay = currentMonth.atEndOfMonth();
                LocalDate targetEndDay = monthEndDay.isAfter(yesterday) ? yesterday : monthEndDay;

                while (!dayIter.isAfter(targetEndDay)) {
                    String dailyUrl = String.format(
                            "%s/daily/klines/%s/%s/%s-%s-%s.zip",
                            BINANCE_VISION_BASE, symbol, intervalName, symbol, intervalName, dayIter.format(DAY_FORMATTER)
                    );

                    byte[] zipBytes = downloadZipBytesInMemory(dailyUrl);
                    if (zipBytes != null) {
                        List<Kline> batch = processZipBytesInMemory(zipBytes, intervalMillis);
                        if (!batch.isEmpty()) {
                            long lastBatchTime = saveKlinesBatch(symbol, intervalName, batch, isNewCoin);
                            isNewCoin = false;
                            filesDownloaded++;
                            totalKlinesSaved += batch.size();
                            latestRecordedTime = Math.max(latestRecordedTime, lastBatchTime);
                            logProgress(String.format("[%s - %s] Saved daily archive %s: %,d klines (Total: %,d)",
                                    symbol, intervalName, dayIter, batch.size(), totalKlinesSaved));
                        }
                    }

                    dayIter = dayIter.plusDays(1);
                }

                currentMonth = currentMonth.plusMonths(1);
            }

            if (filesDownloaded == 0) {
                return new DownloadResult(
                        symbol,
                        intervalName,
                        Status.NO_DATA,
                        0,
                        0,
                        startTimeMillis,
                        latestRecordedTime,
                        Duration.ofMillis(System.currentTimeMillis() - startWallTime),
                        "No archives available for " + symbol + " (" + intervalName + ") in range " + startDate + " to " + yesterday
                );
            }

            return new DownloadResult(
                    symbol,
                    intervalName,
                    Status.SUCCESS,
                    filesDownloaded,
                    totalKlinesSaved,
                    startTimeMillis,
                    latestRecordedTime,
                    Duration.ofMillis(System.currentTimeMillis() - startWallTime),
                    "Successfully downloaded and stored " + totalKlinesSaved + " klines from " + filesDownloaded + " archives"
            );

        } catch (Exception e) {
            return new DownloadResult(
                    symbol,
                    intervalName,
                    Status.ERROR,
                    0,
                    0,
                    0,
                    0,
                    Duration.ofMillis(System.currentTimeMillis() - startWallTime),
                    "Download failed: " + e.getMessage()
            );
        }
    }

    private byte[] downloadZipBytesInMemory(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(requestTimeout)
                .header("User-Agent", "Binance-Downloader/1.0")
                .GET()
                .build();

        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() == 200) {
            return response.body();
        }
        if (response.statusCode() == 404) {
            return null;
        }
        throw new IOException("Unexpected HTTP " + response.statusCode() + " fetching " + url);
    }

    /**
     * Decompresses and parses klines from raw ZIP archive bytes in memory.
     *
     * @param zipBytes raw bytes of the downloaded ZIP file
     * @param interval candle {@link Interval}
     * @return sorted and deduplicated list of parsed {@link Kline} objects
     * @throws IOException if decompression or reading fails
     */
    public List<Kline> processZipBytesInMemory(byte[] zipBytes, Interval interval) throws IOException {
        return processZipBytesInMemory(zipBytes, interval.millis());
    }

    /**
     * Decompresses and parses klines from raw ZIP archive bytes in memory.
     *
     * @param zipBytes raw bytes of the downloaded ZIP file
     * @param interval interval string (e.g. "1m", "5m")
     * @return sorted and deduplicated list of parsed {@link Kline} objects
     * @throws IOException if decompression or reading fails
     */
    public List<Kline> processZipBytesInMemory(byte[] zipBytes, String interval) throws IOException {
        return processZipBytesInMemory(zipBytes, Interval.of(interval).millis());
    }

    /**
     * Decompresses and parses klines from raw ZIP archive bytes in memory.
     * Streams the contained CSV line-by-line using {@link BufferedReader} to minimize memory footprint.
     *
     * @param zipBytes       raw bytes of the downloaded ZIP file
     * @param intervalMillis candle duration in milliseconds
     * @return sorted and deduplicated list of parsed {@link Kline} objects
     * @throws IOException if decompression or reading fails
     */
    public List<Kline> processZipBytesInMemory(byte[] zipBytes, long intervalMillis) throws IOException {
        List<Kline> klines = new ArrayList<>();

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().endsWith(".csv")) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        Kline kline = parseCsvLine(line.trim(), intervalMillis);
                        if (kline != null) {
                            klines.add(kline);
                        }
                    }
                }
                zis.closeEntry();
            }
        }

        if (klines.isEmpty()) {
            return Collections.emptyList();
        }

        klines.sort(Comparator.comparingLong(Kline::closeTime));

        List<Kline> deduplicated = new ArrayList<>(klines.size());
        Kline prev = null;
        for (Kline curr : klines) {
            if (prev == null || curr.closeTime() != prev.closeTime()) {
                deduplicated.add(curr);
                prev = curr;
            }
        }

        return deduplicated;
    }

    /**
     * Parses a single CSV line from a Binance Vision archive into a {@link Kline}.
     *
     * @param line     raw CSV line string
     * @param interval candle {@link Interval}
     * @return parsed {@link Kline}, or null if the line is a header, comment, or invalid
     */
    public static Kline parseCsvLine(String line, Interval interval) {
        return parseCsvLine(line, interval.millis());
    }

    /**
     * Parses a single CSV line from a Binance Vision archive into a {@link Kline}.
     *
     * @param line     raw CSV line string
     * @param interval interval string (e.g. "1m", "5m")
     * @return parsed {@link Kline}, or null if the line is a header, comment, or invalid
     */
    public static Kline parseCsvLine(String line, String interval) {
        return parseCsvLine(line, Interval.of(interval).millis());
    }

    /**
     * Parses a single CSV line from a Binance Vision archive into a {@link Kline}.
     * Handles both formats (with and without explicit close_time column).
     *
     * @param line           raw CSV line string
     * @param intervalMillis candle duration in milliseconds
     * @return parsed {@link Kline}, or null if the line is a header, comment, or invalid
     */
    public static Kline parseCsvLine(String line, long intervalMillis) {
        if (line == null || line.isEmpty()) return null;
        char firstChar = line.charAt(0);
        if (!Character.isDigit(firstChar)) {
            return null;
        }

        String[] tokens = line.split(",", -1);
        if (tokens.length < 6) return null;

        try {
            double open = Double.parseDouble(tokens[1].trim());
            double high = Double.parseDouble(tokens[2].trim());
            double low = Double.parseDouble(tokens[3].trim());
            double close = Double.parseDouble(tokens[4].trim());
            double volume = Double.parseDouble(tokens[5].trim());

            long rawTimestamp;
            if (tokens.length >= 7 && !tokens[6].isBlank()) {
                rawTimestamp = Long.parseLong(tokens[6].trim());
            } else {
                rawTimestamp = Long.parseLong(tokens[0].trim()) + intervalMillis;
            }

            long normalizedCloseTime = normalizeTimestamp(rawTimestamp);
            return new Kline(open, high, low, close, volume, normalizedCloseTime);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Normalizes a raw timestamp (seconds, milliseconds, or microseconds) to epoch milliseconds
     * aligned to exact second boundaries (e.g. {@code :59.999} -> next {@code :00.000}).
     *
     * @param ts raw timestamp in seconds, milliseconds, or microseconds
     * @return normalized epoch milliseconds
     */
    public static long normalizeTimestamp(long ts) {
        final long maxSeconds = 1_000_000_000_000L;
        final long maxMillis = 1_000_000_000_000_000L;
        final long msTs;

        if (ts < maxSeconds) {
            msTs = ts * 1000L;
        } else if (ts < maxMillis) {
            msTs = ts;
        } else {
            msTs = ts / 1000L;
        }

        return ((msTs + 500L) / 1000L) * 1000L;
    }

    private long saveKlinesBatch(String symbol, String interval, List<Kline> klines, boolean isNewCoin) throws IOException {
        if (klines.isEmpty()) return 0;

        long firstBatchTime = klines.get(0).closeTime();
        long lastBatchTime = klines.get(klines.size() - 1).closeTime();
        long intervalMillis = Interval.of(interval).millis();

        if (isNewCoin) {
            Metadata meta = new Metadata(firstBatchTime, lastBatchTime, intervalMillis, symbol);
            storage.writeMetadata(symbol, interval, meta);
        }

        storage.writeKlines(symbol, interval, klines);
        storage.updateMostRecentKlineTime(symbol, interval, lastBatchTime);

        return lastBatchTime;
    }

    /**
     * Downloads historical klines for multiple coins concurrently.
     *
     * @param coins       collection of coin symbols (e.g. ["BTCUSDT", "ETHUSDT"])
     * @param interval    the candle interval
     * @param parallelism thread pool size
     * @return map of symbol to {@link DownloadResult}
     */
    public Map<String, DownloadResult> downloadCoinsConcurrently(Collection<String> coins, Interval interval, int parallelism) {
        return downloadCoinsConcurrently(coins, interval.name(), parallelism);
    }

    /**
     * Downloads historical klines for multiple coins concurrently.
     *
     * @param coins       collection of coin symbols (e.g. ["BTCUSDT", "ETHUSDT"])
     * @param interval    the candle interval string (e.g. "1m", "5m")
     * @param parallelism thread pool size
     * @return map of symbol to {@link DownloadResult}
     */
    public Map<String, DownloadResult> downloadCoinsConcurrently(Collection<String> coins, String interval, int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be positive, got " + parallelism);
        }
        Map<String, DownloadResult> results = new ConcurrentHashMap<>();

        try (ExecutorService executor = Executors.newFixedThreadPool(parallelism)) {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String coin : coins) {
                futures.add(CompletableFuture.runAsync(() -> {
                    try {
                        results.put(coin, downloadCoin(coin, interval));
                    } catch (Exception e) {
                        results.put(coin, new DownloadResult(
                                coin, interval, Status.ERROR, 0, 0, 0, 0,
                                Duration.ZERO, "Download failed: " + e.getMessage()));
                    }
                }, executor));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        }

        return results;
    }
}
