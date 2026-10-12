package com.lukaulcar.example;

import com.lukaulcar.downloader.BinanceDownloader;
import com.lukaulcar.model.Kline;
import com.lukaulcar.storage.KlineStorage;
import com.lukaulcar.storage.Metadata;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.stream.Stream;

/**
 * Example demonstrating how to initialize storage, download historical klines,
 * inspect metadata, and stream klines directly from disk.
 * <p>
 * This file is located in the test directory and is excluded from the released Maven JAR.
 */
public class ExampleUsage {

    public static void main(String[] args) throws IOException {
        String symbol = "BTCUSDT";
        String interval = "5m";

        KlineStorage storage = new KlineStorage(Path.of("storage"));

        BinanceDownloader downloader = new BinanceDownloader(storage, System.out::println);
        BinanceDownloader.DownloadResult result = downloader.downloadCoin(symbol, interval);
        System.out.println("Result: " + result.status() + " - " + result.message());

        if (storage.hasData(symbol, interval)) {
            Metadata meta = storage.readMetadata(symbol, interval);
            System.out.println("Range: " + Instant.ofEpochMilli(meta.firstKlineTime())
                    + " to " + Instant.ofEpochMilli(meta.mostRecentKlineTime()));

            System.out.println("Stored symbols: " + storage.getStoredSymbols());
            System.out.println("Stored intervals for " + symbol + ": " + storage.getStoredIntervals(symbol));

            try (Stream<Kline> stream = storage.streamAllKlines(symbol, interval)) {
                stream.limit(10).forEach(k -> System.out.println("  " + k));
            }
        }
    }
}
