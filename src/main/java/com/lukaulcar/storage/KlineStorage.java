package com.lukaulcar.storage;

import com.lukaulcar.model.Interval;
import com.lukaulcar.model.Kline;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Thread-safe, direct-seek binary storage engine for candlestick (kline) data.
 * <p>
 * Stores each coin and interval in a separate binary file (e.g. {@code BTCUSDT/BTCUSDT_1m.bin})
 * consisting of a variable-length metadata header followed by contiguous 48-byte {@link Kline} slots.
 * This structure allows instant $O(1)$ random seeking and sequential streaming without loading entire datasets into memory.
 */
public class KlineStorage implements Closeable {

    private static final int CHUNK_READ_BUFFER_SIZE = 512 * 1024;

    private final Path basePath;
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> fileLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Metadata> metadataCache = new ConcurrentHashMap<>();
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * Initializes KlineStorage rooted at the given base directory.
     * Automatically scans and pre-loads all existing metadata headers into memory.
     *
     * @param basePath root directory for storing coin binary files
     * @throws IOException if directory creation or scanning fails
     */
    public KlineStorage(Path basePath) throws IOException {
        this.basePath = basePath;
        Files.createDirectories(basePath);
        loadAllMetadataToCache();
    }

    private String getCacheKey(String symbol, String interval) {
        return symbol.toUpperCase() + "/" + interval.toLowerCase();
    }

    private Path getFilePath(String symbol, String interval) {
        String cleanSymbol = symbol.toUpperCase();
        String cleanInterval = interval.toLowerCase();
        return basePath.resolve(cleanSymbol).resolve(cleanSymbol + "_" + cleanInterval + ".bin");
    }

    private ReentrantReadWriteLock getFileLock(String key) {
        return fileLocks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + buffer.position());
            if (read <= 0) {
                throw new IOException("Unexpected end of file at position " + position);
            }
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            position += channel.write(buffer, position);
        }
    }

    private void loadAllMetadataToCache() {
        try {
            if (!Files.exists(basePath)) return;
            try (DirectoryStream<Path> symbolDirs = Files.newDirectoryStream(basePath, Files::isDirectory)) {
                for (Path symbolDir : symbolDirs) {
                    try (DirectoryStream<Path> files = Files.newDirectoryStream(symbolDir, "*.bin")) {
                        for (Path file : files) {
                            String fileName = file.getFileName().toString();
                            String baseName = fileName.substring(0, fileName.length() - 4);
                            int underscore = baseName.lastIndexOf('_');
                            if (underscore != -1) {
                                String symbol = baseName.substring(0, underscore);
                                String interval = baseName.substring(underscore + 1);
                                try {
                                    readMetadata(symbol, interval);
                                } catch (Exception ignored) {
                                }
                            }
                        }
                    }
                }
            }
        } catch (IOException ignored) {
        }
    }

    /**
     * Writes or overwrites the metadata header for the specified coin and interval.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m", "5m")
     * @param metadata the {@link Metadata} header to persist
     * @throws IOException if a disk write error occurs
     */
    public void writeMetadata(String symbol, String interval, Metadata metadata) throws IOException {
        ensureNotShutdown();
        Path filePath = getFilePath(symbol, interval);
        Files.createDirectories(filePath.getParent());

        String key = getCacheKey(symbol, interval);
        ReentrantReadWriteLock lock = getFileLock(key);
        lock.writeLock().lock();
        try {
            if (Files.exists(filePath) && Files.size(filePath) >= metadata.size()) {
                Metadata stored = readHeaderUnchecked(filePath);
                if (stored != null && stored.equals(metadata)) {
                    metadataCache.put(key, metadata);
                    return;
                }
                if (stored != null && stored.size() != metadata.size()) {
                    throw new IOException("Header size mismatch for " + key + ", refusing to overwrite");
                }
            }

            try (FileChannel channel = FileChannel.open(filePath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.allocate(metadata.size()).order(ByteOrder.BIG_ENDIAN);
                metadata.writeTo(buffer);
                buffer.flip();
                writeFully(channel, buffer, 0);
            }

            metadataCache.put(key, metadata);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static Metadata readHeaderUnchecked(Path filePath) {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long size = Files.size(filePath);
            if (size < Metadata.MIN_SIZE) {
                return null;
            }
            int readLimit = (int) Math.min(size, 1024L);
            ByteBuffer buffer = ByteBuffer.allocate(readLimit).order(ByteOrder.BIG_ENDIAN);
            readFully(channel, buffer, 0);
            buffer.flip();
            return Metadata.readFrom(buffer);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Reads the metadata header for the specified coin and interval.
     * Checks memory cache first, falling back to disk read.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m", "5m")
     * @return the cached or parsed {@link Metadata}
     * @throws NoSuchFileException if no data exists on disk
     * @throws IOException         if a file read error occurs
     */
    public Metadata readMetadata(String symbol, String interval) throws IOException {
        String key = getCacheKey(symbol, interval);
        Metadata cached = metadataCache.get(key);
        if (cached != null) {
            return cached;
        }

        Path filePath = getFilePath(symbol, interval);
        if (!Files.exists(filePath)) {
            throw new NoSuchFileException("Metadata file does not exist for " + key);
        }

        ReentrantReadWriteLock lock = getFileLock(key);
        lock.readLock().lock();
        try {
            long size = Files.size(filePath);
            if (size < Metadata.MIN_SIZE) {
                throw new IOException("File does not contain valid metadata header: " + key + " (size: " + size + ")");
            }

            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                int readLimit = (int) Math.min(size, 1024L);
                ByteBuffer buffer = ByteBuffer.allocate(readLimit).order(ByteOrder.BIG_ENDIAN);
                readFully(channel, buffer, 0);
                buffer.flip();
                Metadata metadata = Metadata.readFrom(buffer);
                metadataCache.put(key, metadata);
                return metadata;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Updates the {@code lastApiFetchTime} field in-place on disk (offset 8) without rewriting klines.
     *
     * @param symbol           trading symbol (e.g. "BTCUSDT")
     * @param interval         interval string (e.g. "1m")
     * @param lastApiFetchTime timestamp in epoch milliseconds
     * @throws IOException if a disk write error occurs
     */
    public void updateLastApiFetchTime(String symbol, String interval, long lastApiFetchTime) throws IOException {
        ensureNotShutdown();
        Metadata current = readMetadata(symbol, interval);
        if (current.firstKlineTime() == 0) {
            throw new IllegalStateException("Metadata does not exist for " + symbol + " (" + interval + "), cannot update");
        }

        Path filePath = getFilePath(symbol, interval);
        String key = getCacheKey(symbol, interval);
        ReentrantReadWriteLock lock = getFileLock(key);
        lock.writeLock().lock();
        try {
            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                buffer.putLong(lastApiFetchTime);
                buffer.flip();
                writeFully(channel, buffer, 8);
            }
            Metadata updated = new Metadata(
                    current.firstKlineTime(),
                    lastApiFetchTime,
                    current.intervalMillis(),
                    current.coin(),
                    current.size()
            );
            metadataCache.put(key, updated);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates the most recent kline timestamp in the metadata header on disk.
     *
     * @param symbol             trading symbol (e.g. "BTCUSDT")
     * @param interval           interval string (e.g. "1m")
     * @param mostRecentKlineTime timestamp in epoch milliseconds
     * @throws IOException if a disk write error occurs
     */
    public void updateMostRecentKlineTime(String symbol, String interval, long mostRecentKlineTime) throws IOException {
        updateLastApiFetchTime(symbol, interval, mostRecentKlineTime);
    }

    /**
     * Writes a batch of klines directly to their corresponding disk offset slots.
     * Calculates the slot position as {@code (closeTime - firstKlineTime) / intervalMillis}.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m")
     * @param klines   list of klines to store
     * @throws IOException if a disk write error occurs
     */
    public void writeKlines(String symbol, String interval, List<Kline> klines) throws IOException {
        ensureNotShutdown();
        if (klines == null || klines.isEmpty()) {
            return;
        }

        String key = getCacheKey(symbol, interval);
        Path filePath = getFilePath(symbol, interval);
        long targetIntervalMillis = Interval.of(interval).millis();

        Metadata metadata;
        try {
            metadata = readMetadata(symbol, interval);
        } catch (NoSuchFileException e) {
            long earliest = Long.MAX_VALUE;
            for (Kline k : klines) {
                if (k.closeTime() > 0 && k.closeTime() < earliest) {
                    earliest = k.closeTime();
                }
            }
            if (earliest == Long.MAX_VALUE) {
                return;
            }
            metadata = new Metadata(earliest, 0, targetIntervalMillis, symbol.toUpperCase());
            writeMetadata(symbol, interval, metadata);
        }

        long firstKlineTime = metadata.firstKlineTime();
        if (firstKlineTime == 0) {
            throw new IllegalStateException("firstKlineTime is 0 for " + key);
        }

        long intervalMillis = metadata.intervalMillis() > 0 ? metadata.intervalMillis() : targetIntervalMillis;
        int headerSize = metadata.size();

        List<Kline> sorted = new ArrayList<>(klines);
        sorted.sort(Comparator.comparingLong(Kline::closeTime));

        ReentrantReadWriteLock lock = getFileLock(key);
        lock.writeLock().lock();
        try {
            Files.createDirectories(filePath.getParent());
            try (FileChannel channel = FileChannel.open(filePath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {

                int i = 0;
                while (i < sorted.size()) {
                    Kline first = sorted.get(i);
                    if (first.closeTime() < firstKlineTime) {
                        i++;
                        continue;
                    }

                    long startSlot = (first.closeTime() - firstKlineTime) / intervalMillis;
                    long startOffset = headerSize + (startSlot * Kline.BYTES);

                    int runEnd = i + 1;
                    while (runEnd < sorted.size()) {
                        Kline curr = sorted.get(runEnd);
                        long expectedCloseTime = first.closeTime() + (long) (runEnd - i) * intervalMillis;
                        if (curr.closeTime() != expectedCloseTime) {
                            break;
                        }
                        runEnd++;
                    }

                    int runLength = runEnd - i;
                    int bufferCapacity = runLength * Kline.BYTES;
                    ByteBuffer chunkBuffer = ByteBuffer.allocate(bufferCapacity).order(ByteOrder.LITTLE_ENDIAN);

                    for (int j = i; j < runEnd; j++) {
                        sorted.get(j).writeTo(chunkBuffer);
                    }

                    chunkBuffer.flip();
                    writeFully(channel, chunkBuffer, startOffset);

                    i = runEnd;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Reads a list of klines within the specified timestamp range.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m", "5m")
     * @param from     starting timestamp in epoch millis (inclusive). Pass {@code <= 0} to start from the earliest candle.
     * @param to       ending timestamp in epoch millis (exclusive). Pass {@code <= 0} to read through the latest candle.
     * @param limit    maximum number of candles to return, or {@code <= 0} for unlimited
     * @return list of parsed {@link Kline} objects in chronological order
     * @throws IOException if a disk read error occurs
     */
    public List<Kline> getKlines(String symbol, String interval, long from, long to, int limit) throws IOException {
        Metadata metadata;
        try {
            metadata = readMetadata(symbol, interval);
        } catch (NoSuchFileException e) {
            return Collections.emptyList();
        }

        long firstKlineTime = metadata.firstKlineTime();
        if (firstKlineTime == 0) {
            return Collections.emptyList();
        }

        Path filePath = getFilePath(symbol, interval);
        if (!Files.exists(filePath)) {
            return Collections.emptyList();
        }

        long intervalMillis = metadata.intervalMillis() > 0 ? metadata.intervalMillis() : Interval.of(interval).millis();
        int headerSize = metadata.size();

        String key = getCacheKey(symbol, interval);
        ReentrantReadWriteLock lock = getFileLock(key);
        lock.readLock().lock();
        try {
            long fileLength = Files.size(filePath);
            if (fileLength <= headerSize) {
                return Collections.emptyList();
            }

            long totalKlines = (fileLength - headerSize) / Kline.BYTES;
            long startIndex = (from <= 0) ? 0 : (from - firstKlineTime) / intervalMillis;
            long endIndex = (to <= 0) ? totalKlines : (to - firstKlineTime) / intervalMillis;

            if (startIndex < 0) startIndex = 0;
            if (endIndex > totalKlines) endIndex = totalKlines;
            if (startIndex >= endIndex) {
                return Collections.emptyList();
            }

            long klinesToRead = endIndex - startIndex;
            if (limit > 0 && klinesToRead > limit) {
                if (from <= 0 && to <= 0) {
                    startIndex = endIndex - limit;
                } else {
                    endIndex = startIndex + limit;
                }
                klinesToRead = endIndex - startIndex;
            }
            if (klinesToRead <= 0) {
                return Collections.emptyList();
            }

            List<Kline> results = new ArrayList<>((int) Math.min(klinesToRead, 100_000));

            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                long currentFilePos = headerSize + (startIndex * Kline.BYTES);
                long bytesRemaining = klinesToRead * Kline.BYTES;

                int bufferSize = (int) Math.min(bytesRemaining, CHUNK_READ_BUFFER_SIZE);
                bufferSize = (bufferSize / Kline.BYTES) * Kline.BYTES;
                if (bufferSize < Kline.BYTES) bufferSize = Kline.BYTES;

                ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);

                while (bytesRemaining > 0) {
                    int toReadThisChunk = (int) Math.min(bytesRemaining, bufferSize);
                    buffer.clear();
                    buffer.limit(toReadThisChunk);

                    while (buffer.hasRemaining()) {
                        int bytesRead = channel.read(buffer, currentFilePos + buffer.position());
                        if (bytesRead <= 0) {
                            break;
                        }
                    }
                    if (buffer.position() == 0) {
                        break;
                    }

                    buffer.flip();
                    while (buffer.remaining() >= Kline.BYTES) {
                        Kline kline = Kline.readFrom(buffer);
                        if (kline.isValid()) {
                            results.add(kline);
                        }
                    }

                    int consumed = buffer.position();
                    currentFilePos += consumed;
                    bytesRemaining -= consumed;
                    if (consumed < toReadThisChunk) {
                        break;
                    }
                }
            }

            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Streams klines chronologically directly from disk.
     * <p>
     * <b>Important:</b> The returned stream holds an open file read lock.
     * Always enclose in a {@code try-with-resources} block to ensure the lock is unlocked:
     * <pre>{@code
     * try (Stream<Kline> stream = storage.streamKlines("BTCUSDT", "1m", from, to)) {
     *     stream.forEach(k -> ...);
     * }
     * }</pre>
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m", "5m")
     * @param from     starting timestamp in epoch millis (inclusive). Pass {@code <= 0} to start from the earliest candle.
     * @param to       ending timestamp in epoch millis (exclusive). Pass {@code <= 0} to read through the latest candle.
     * @return auto-closeable {@link Stream} of klines
     * @throws IOException if a disk open error occurs
     */
    public Stream<Kline> streamKlines(String symbol, String interval, long from, long to) throws IOException {
        KlineIterator iterator = iterateKlines(symbol, interval, from, to);
        Spliterator<Kline> spliterator = Spliterators.spliteratorUnknownSize(
                iterator,
                Spliterator.ORDERED | Spliterator.NONNULL
        );
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }

    /**
     * Streams klines chronologically directly from disk.
     * <p>
     * <b>Important:</b> Enclose in a {@code try-with-resources} block to release the underlying read lock.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval candle {@link Interval}
     * @param from     starting timestamp in epoch millis (inclusive). Pass {@code <= 0} for earliest.
     * @param to       ending timestamp in epoch millis (exclusive). Pass {@code <= 0} for latest.
     * @return auto-closeable {@link Stream} of klines
     * @throws IOException if a disk open error occurs
     */
    public Stream<Kline> streamKlines(String symbol, Interval interval, long from, long to) throws IOException {
        return streamKlines(symbol, interval.name(), from, to);
    }

    /**
     * Creates an iterator over the stored klines in the specified time range.
     * <p>
     * <b>Important:</b> Holds an open read lock on the storage file.
     * Always enclose in a {@code try-with-resources} statement to guarantee the lock is released:
     * <pre>{@code
     * try (KlineIterator it = storage.iterateKlines("BTCUSDT", "1m", from, to)) {
     *     while (it.hasNext()) {
     *         Kline k = it.next();
     *     }
     * }
     * }</pre>
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m", "5m")
     * @param from     starting timestamp in epoch millis (inclusive). Pass {@code <= 0} for earliest.
     * @param to       ending timestamp in epoch millis (exclusive). Pass {@code <= 0} for latest.
     * @return auto-closeable {@link KlineIterator}
     * @throws IOException if a disk read error occurs
     */
    public KlineIterator iterateKlines(String symbol, String interval, long from, long to) throws IOException {
        Metadata metadata;
        try {
            metadata = readMetadata(symbol, interval);
        } catch (NoSuchFileException e) {
            return emptyKlineIterator();
        }

        long firstKlineTime = metadata.firstKlineTime();
        if (firstKlineTime == 0) {
            return emptyKlineIterator();
        }

        Path filePath = getFilePath(symbol, interval);
        if (!Files.exists(filePath)) {
            return emptyKlineIterator();
        }

        long intervalMillis = metadata.intervalMillis() > 0 ? metadata.intervalMillis() : Interval.of(interval).millis();
        int headerSize = metadata.size();

        String key = getCacheKey(symbol, interval);
        ReentrantReadWriteLock lock = getFileLock(key);
        lock.readLock().lock();
        try {
            long fileLength = Files.size(filePath);
            if (fileLength <= headerSize) {
                lock.readLock().unlock();
                return emptyKlineIterator();
            }

            long totalKlines = (fileLength - headerSize) / Kline.BYTES;
            long startIndex = (from <= 0) ? 0 : (from - firstKlineTime) / intervalMillis;
            long endIndex = (to <= 0) ? totalKlines : (to - firstKlineTime) / intervalMillis;

            if (startIndex < 0) startIndex = 0;
            if (endIndex > totalKlines) endIndex = totalKlines;
            if (startIndex >= endIndex) {
                lock.readLock().unlock();
                return emptyKlineIterator();
            }

            long startFilePos = headerSize + (startIndex * Kline.BYTES);
            long endFilePos = headerSize + (endIndex * Kline.BYTES);

            FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ);
            return new KlineIterator(channel, lock.readLock(), startFilePos, endFilePos, CHUNK_READ_BUFFER_SIZE);
        } catch (Exception e) {
            lock.readLock().unlock();
            throw e;
        }
    }

    /**
     * Creates an iterator over the stored klines in the specified time range.
     * <p>
     * <b>Important:</b> Always enclose in a {@code try-with-resources} statement to release the read lock.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval candle {@link Interval}
     * @param from     starting timestamp in epoch millis (inclusive). Pass {@code <= 0} for earliest.
     * @param to       ending timestamp in epoch millis (exclusive). Pass {@code <= 0} for latest.
     * @return auto-closeable {@link KlineIterator}
     * @throws IOException if a disk read error occurs
     */
    public KlineIterator iterateKlines(String symbol, Interval interval, long from, long to) throws IOException {
        return iterateKlines(symbol, interval.name(), from, to);
    }

    private static KlineIterator emptyKlineIterator() {
        return new KlineIterator(null, null, 0, 0, 0) {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Kline next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * Reads a single kline at an exact close timestamp via direct $O(1)$ offset lookup on disk.
     *
     * @param symbol    trading symbol (e.g. "BTCUSDT")
     * @param interval  interval string (e.g. "1m")
     * @param closeTime exact candle close timestamp (epoch millis)
     * @return {@link Optional} containing the kline if present and non-zero
     * @throws IOException if a disk read error occurs
     */
    public Optional<Kline> getKline(String symbol, String interval, long closeTime) throws IOException {
        Metadata metadata;
        try {
            metadata = readMetadata(symbol, interval);
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }

        long firstKlineTime = metadata.firstKlineTime();
        if (firstKlineTime == 0 || closeTime < firstKlineTime) {
            return Optional.empty();
        }

        long intervalMillis = metadata.intervalMillis() > 0 ? metadata.intervalMillis() : Interval.of(interval).millis();
        long slotIndex = (closeTime - firstKlineTime) / intervalMillis;
        long offset = metadata.size() + (slotIndex * Kline.BYTES);

        Path filePath = getFilePath(symbol, interval);
        if (!Files.exists(filePath)) {
            return Optional.empty();
        }

        String key = getCacheKey(symbol, interval);
        ReentrantReadWriteLock lock = getFileLock(key);
        lock.readLock().lock();
        try {
            if (Files.size(filePath) < offset + Kline.BYTES) {
                return Optional.empty();
            }

            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.allocate(Kline.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                readFully(channel, buffer, offset);
                buffer.flip();
                Kline kline = Kline.readFrom(buffer);
                return kline.isValid() ? Optional.of(kline) : Optional.empty();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Reads the last (most recent) non-zero kline currently stored on disk.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m")
     * @return {@link Optional} containing the last stored kline, or empty if no data exists
     * @throws IOException if a disk read error occurs
     */
    public Optional<Kline> getLastKline(String symbol, String interval) throws IOException {
        Metadata metadata;
        try {
            metadata = readMetadata(symbol, interval);
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }

        Path filePath = getFilePath(symbol, interval);
        if (!Files.exists(filePath)) {
            return Optional.empty();
        }

        int headerSize = metadata.size();
        String key = getCacheKey(symbol, interval);
        ReentrantReadWriteLock lock = getFileLock(key);
        lock.readLock().lock();
        try {
            long size = Files.size(filePath);
            if (size < headerSize + Kline.BYTES) {
                return Optional.empty();
            }

            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.allocate(Kline.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                long offset = size - Kline.BYTES;
                while (offset >= headerSize) {
                    buffer.clear();
                    readFully(channel, buffer, offset);
                    buffer.flip();
                    Kline kline = Kline.readFrom(buffer);
                    if (kline.isValid()) {
                        return Optional.of(kline);
                    }
                    offset -= Kline.BYTES;
                }
            }
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the total capacity in slots (klines) allocated in the storage file.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m")
     * @return number of kline slots currently allocated
     * @throws IOException if a disk query error occurs
     */
    public long getTotalKlineSlots(String symbol, String interval) throws IOException {
        Metadata metadata = readMetadata(symbol, interval);
        Path path = getFilePath(symbol, interval);
        if (!Files.exists(path)) return 0;
        long size = Files.size(path);
        if (size < metadata.size()) return 0;
        return (size - metadata.size()) / Kline.BYTES;
    }

    /**
     * Checks if binary kline data exists for the given symbol and interval.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval candle interval string (e.g. "1m", "5m")
     * @return true if data exists and is valid on disk
     */
    public boolean hasData(String symbol, String interval) {
        String key = getCacheKey(symbol, interval);
        if (metadataCache.containsKey(key)) {
            return true;
        }
        Path path = getFilePath(symbol, interval);
        try {
            return Files.exists(path) && Files.size(path) >= Metadata.MIN_SIZE;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Checks if binary kline data exists for the given symbol and interval.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval candle {@link Interval}
     * @return true if data exists and is valid on disk
     */
    public boolean hasData(String symbol, Interval interval) {
        return hasData(symbol, interval.name());
    }

    /**
     * Returns an unmodifiable set of all unique coin symbols stored on disk.
     *
     * @return sorted set of stored coin symbols
     */
    public Set<String> getStoredSymbols() {
        Set<String> symbols = new TreeSet<>();
        for (String key : metadataCache.keySet()) {
            int slash = key.indexOf('/');
            if (slash != -1) {
                symbols.add(key.substring(0, slash));
            }
        }
        return Collections.unmodifiableSet(symbols);
    }

    /**
     * Returns an unmodifiable list of all stored intervals for a given coin symbol.
     *
     * @param symbol trading symbol (e.g. "BTCUSDT")
     * @return sorted list of available intervals (e.g. ["1m", "5m", "1h"])
     */
    public List<String> getStoredIntervals(String symbol) {
        String prefix = symbol.toUpperCase() + "/";
        List<String> intervals = new ArrayList<>();
        for (String key : metadataCache.keySet()) {
            if (key.startsWith(prefix)) {
                intervals.add(key.substring(prefix.length()));
            }
        }
        Collections.sort(intervals);
        return Collections.unmodifiableList(intervals);
    }

    /**
     * Returns an unmodifiable snapshot map of all cached metadata.
     *
     * @return map of cache key ("SYMBOL/interval") to {@link Metadata}
     */
    public Map<String, Metadata> getAllMetadata() {
        return Collections.unmodifiableMap(new HashMap<>(metadataCache));
    }

    /**
     * Reads all stored klines for a given symbol and interval into memory.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m")
     * @return list of all stored klines ordered chronologically
     * @throws IOException if a disk read error occurs
     */
    public List<Kline> getAllKlines(String symbol, String interval) throws IOException {
        return getKlines(symbol, interval, 0, 0, 0);
    }

    /**
     * Reads all stored klines for a given symbol and interval into memory.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval candle {@link Interval}
     * @return list of all stored klines ordered chronologically
     * @throws IOException if a disk read error occurs
     */
    public List<Kline> getAllKlines(String symbol, Interval interval) throws IOException {
        return getAllKlines(symbol, interval.name());
    }

    /**
     * Streams all stored klines chronologically from disk.
     * The returned {@link Stream} must be enclosed in a try-with-resources statement to release the read lock.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m")
     * @return auto-closeable stream of all stored klines
     * @throws IOException if a disk read error occurs
     */
    public Stream<Kline> streamAllKlines(String symbol, String interval) throws IOException {
        return streamKlines(symbol, interval, 0, 0);
    }

    /**
     * Streams all stored klines chronologically from disk.
     * The returned {@link Stream} must be enclosed in a try-with-resources statement to release the read lock.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval candle {@link Interval}
     * @return auto-closeable stream of all stored klines
     * @throws IOException if a disk read error occurs
     */
    public Stream<Kline> streamAllKlines(String symbol, Interval interval) throws IOException {
        return streamAllKlines(symbol, interval.name());
    }

    /**
     * Deletes the stored binary file for a symbol and interval, removing it from cache and memory.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval interval string (e.g. "1m")
     * @return true if the file existed and was deleted
     * @throws IOException if a file deletion error occurs
     */
    public boolean deleteData(String symbol, String interval) throws IOException {
        ensureNotShutdown();
        String key = getCacheKey(symbol, interval);
        Path filePath = getFilePath(symbol, interval);
        ReentrantReadWriteLock lock = getFileLock(key);
        lock.writeLock().lock();
        try {
            metadataCache.remove(key);
            boolean deleted = Files.deleteIfExists(filePath);
            try {
                Path parent = filePath.getParent();
                if (parent != null && Files.isDirectory(parent)) {
                    try (DirectoryStream<Path> ds = Files.newDirectoryStream(parent)) {
                        if (!ds.iterator().hasNext()) {
                            Files.deleteIfExists(parent);
                        }
                    }
                }
            } catch (Exception ignored) {
            }
            return deleted;
        } finally {
            lock.writeLock().unlock();
            fileLocks.remove(key);
        }
    }

    /**
     * Deletes the stored binary file for a symbol and interval.
     *
     * @param symbol   trading symbol (e.g. "BTCUSDT")
     * @param interval candle {@link Interval}
     * @return true if the file existed and was deleted
     * @throws IOException if a file deletion error occurs
     */
    public boolean deleteData(String symbol, Interval interval) throws IOException {
        return deleteData(symbol, interval.name());
    }

    private void ensureNotShutdown() {
        if (isShutdown.get()) {
            throw new IllegalStateException("KlineStorage has been shut down");
        }
    }

    /**
     * Closes KlineStorage, clearing metadata caches and releasing internal state.
     */
    @Override
    public void close() {
        if (isShutdown.compareAndSet(false, true)) {
            metadataCache.clear();
            fileLocks.clear();
        }
    }
}
