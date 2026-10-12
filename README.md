# Binance Downloader

[![Java](https://img.shields.io/badge/Java-21%2B-blue.svg)](https://openjdk.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Dependencies](https://img.shields.io/badge/Dependencies-Zero-brightgreen.svg)]()

A lightweight, zero-dependency Java 21 library for downloading, streaming, and managing Binance historical candlestick (kline) data.

Built specifically for algorithmic trading and backtesting engines that require fast, direct-disk random access without database overhead.

---

## Features

- **Zero Dependencies**: Pure Java 21 utilizing Java NIO `FileChannel` and `java.net.http.HttpClient`.
- **Ultra-Fast Binary Storage**: Compact 48-byte records per candle with direct offset random seeking:
  $$\text{offset} = \text{headerSize} + \left(\frac{\text{closeTime} - \text{firstKlineTime}}{\text{intervalMillis}}\right) \times 48$$
- **Smart Archive Downloading**: Downloads official monthly and daily archives from Binance Vision, automatically falling back to daily archives when monthly archives are not yet published.
- **Incremental Updates**: Automatically detects previously downloaded data and only downloads new archives. Skips downloads if data is already up to date within the last 24 hours.
- **Concurrent Processing**: Multi-threaded downloader with configurable parallelism and optional progress callbacks.
- **Resource Safe**: Read/write locks ensure thread safety across concurrent readers and writers within the same JVM process.

---

## Installation (Maven)

1. Add the JitPack repository to your `pom.xml`:
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

2. Add the dependency:
```xml
<dependency>
    <groupId>com.github.lukaulcar</groupId>
    <artifactId>binance-downloader</artifactId>
    <version>v1.0.0</version>
</dependency>
```
---

## Quickstart Guide

### 1. Initialize Storage & Downloader

```java
import com.lukaulcar.downloader.BinanceDownloader;
import com.lukaulcar.model.Interval;
import com.lukaulcar.storage.KlineStorage;
import java.nio.file.Path;

KlineStorage storage = new KlineStorage(Path.of("storage"));

// Pass an optional progress listener (e.g., System.out::println or SLF4J logger)
BinanceDownloader downloader = new BinanceDownloader(storage, System.out::println);
```

### 2. Download Klines

```java
// Download historical archives for a single coin
BinanceDownloader.DownloadResult result = downloader.downloadCoin("BTCUSDT", "5m");
System.out.println("Status: " + result.status() + " (" + result.klinesSaved() + " klines saved)");

// Download multiple coins concurrently
List<String> symbols = List.of("BTCUSDT", "ETHUSDT", "SOLUSDT");
var results = downloader.downloadCoinsConcurrently(symbols, Interval.ONE_MINUTE, 4);
```

### 3. Read & Stream Klines

```java
import com.lukaulcar.model.Kline;
import java.util.List;
import java.util.stream.Stream;

// Read all klines into memory
List<Kline> klines = storage.getAllKlines("BTCUSDT", "5m");

// Read a specific time range (from is inclusive, to is exclusive; pass 0 for unbounded)
long from = 1704067200000L; // 2024-01-01 00:00:00 UTC
long to   = 1704153600000L; // 2024-01-02 00:00:00 UTC
List<Kline> slice = storage.getKlines("BTCUSDT", "5m", from, to, 1000);

// Stream large datasets directly from disk with low memory footprint
// IMPORTANT: Always use try-with-resources to release the read lock!
try (Stream<Kline> stream = storage.streamAllKlines("BTCUSDT", "5m")) {
    stream.forEach(k -> {
        long openTime = k.openTime(Interval.FIVE_MINUTES);
        System.out.printf("Open: %d | Close: %.2f%n", openTime, k.close());
    });
}
```

### 4. Manage Stored Data

```java
// Check if data exists
boolean exists = storage.hasData("BTCUSDT", "1m");

// Discover available coins and timeframes
Set<String> coins = storage.getStoredSymbols();       // e.g. ["BTCUSDT", "ETHUSDT"]
List<String> timeframes = storage.getStoredIntervals("BTCUSDT"); // e.g. ["1m", "5m", "1h"]

// Delete stored data if needed
storage.deleteData("BTCUSDT", "1m");
```

---

## Data Structure

Each candle is stored on disk as a compact 48-byte binary record (Little Endian):

| Field | Type | Size | Description |
| :--- | :--- | :--- | :--- |
| `open` | `double` (IEEE 754) | 8 bytes | Open price |
| `high` | `double` (IEEE 754) | 8 bytes | High price |
| `low` | `double` (IEEE 754) | 8 bytes | Low price |
| `close` | `double` (IEEE 754) | 8 bytes | Close price |
| `volume` | `double` (IEEE 754) | 8 bytes | Base asset volume |
| `closeTime` | `long` (signed) | 8 bytes | Normalized close timestamp (epoch millis) |

---

## License

This project is licensed under the [MIT License](LICENSE).
