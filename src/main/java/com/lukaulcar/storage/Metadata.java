package com.lukaulcar.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Binary header metadata stored at the beginning of each kline {@code .bin} storage file.
 * Records the anchor start timestamp, latest API fetch timestamp, candle duration, and coin symbol.
 *
 * @param firstKlineTime    the anchor timestamp (epoch millis) representing slot 0
 * @param lastApiFetchTime  the timestamp of the most recent kline or API fetch
 * @param intervalMillis    duration of each candle in milliseconds
 * @param coin              the coin or trading pair symbol (e.g. "BTCUSDT")
 * @param headerSize        the total byte size occupied by this header on disk
 */
public record Metadata(
        long firstKlineTime,
        long lastApiFetchTime,
        long intervalMillis,
        String coin,
        int headerSize
) {
    /**
     * Minimum possible header size in bytes (24 bytes numeric fields + 1 byte name length).
     */
    public static final int MIN_SIZE = 25;

    /**
     * Constructs a Metadata instance with default computed header size based on the coin name length.
     *
     * @param firstKlineTime   anchor timestamp representing slot 0
     * @param lastApiFetchTime timestamp of the most recent kline
     * @param intervalMillis   duration of each candle in milliseconds
     * @param coin             coin or pair symbol
     */
    public Metadata(long firstKlineTime, long lastApiFetchTime, long intervalMillis, String coin) {
        this(firstKlineTime, lastApiFetchTime, intervalMillis, coin,
                25 + coin.getBytes(StandardCharsets.UTF_8).length);
    }

    /**
     * Compact constructor validating coin constraints.
     */
    public Metadata {
        Objects.requireNonNull(coin, "coin cannot be null");
        if (coin.getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("Coin name cannot exceed 255 UTF-8 bytes: " + coin);
        }
    }

    /**
     * Returns the serialized byte size of this metadata header.
     *
     * @return header size in bytes
     */
    public int size() {
        if (headerSize > 0) {
            return headerSize;
        }
        byte[] coinBytes = coin.getBytes(StandardCharsets.UTF_8);
        return 25 + coinBytes.length;
    }

    /**
     * Returns the timestamp of the most recently downloaded kline.
     * Alias for {@link #lastApiFetchTime()}.
     *
     * @return latest kline timestamp (epoch millis)
     */
    public long mostRecentKlineTime() {
        return lastApiFetchTime;
    }

    /**
     * Serializes this metadata into a byte array in Big Endian order.
     *
     * @return byte array containing the serialized header
     */
    public byte[] toBytes() {
        byte[] bytes = new byte[size()];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        writeTo(buffer);
        return bytes;
    }

    /**
     * Writes this metadata's fields into the provided {@link ByteBuffer} in Big Endian order.
     *
     * @param buffer byte buffer with at least {@link #size()} bytes remaining
     */
    public void writeTo(ByteBuffer buffer) {
        byte[] coinBytes = coin.getBytes(StandardCharsets.UTF_8);

        buffer.putLong(firstKlineTime);
        buffer.putLong(lastApiFetchTime);
        buffer.putLong(intervalMillis);

        buffer.put((byte) coinBytes.length);
        buffer.put(coinBytes);
    }

    /**
     * Deserializes a Metadata instance from a raw byte array.
     *
     * @param data byte array containing at least {@link #MIN_SIZE} bytes
     * @return the deserialized {@link Metadata}
     * @throws NullPointerException     if {@code data} is null
     * @throws IllegalArgumentException if {@code data.length} is less than {@link #MIN_SIZE}
     */
    public static Metadata fromBytes(byte[] data) {
        Objects.requireNonNull(data, "data array cannot be null");
        if (data.length < MIN_SIZE) {
            throw new IllegalArgumentException("Expected at least " + MIN_SIZE + " bytes, got " + data.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        return readFrom(buffer);
    }

    /**
     * Reads a Metadata header from the given {@link ByteBuffer} at its current position.
     * Supports both the current compact format and legacy headers containing an exchange tag.
     *
     * @param buffer byte buffer with at least {@link #MIN_SIZE} bytes remaining
     * @return the parsed {@link Metadata}
     * @throws IllegalArgumentException if the buffer has fewer than {@link #MIN_SIZE} bytes or is truncated
     */
    public static Metadata readFrom(ByteBuffer buffer) {
        if (buffer.remaining() < MIN_SIZE) {
            throw new IllegalArgumentException("Buffer has fewer than " + MIN_SIZE + " bytes remaining");
        }
        int startPos = buffer.position();
        long firstKlineTime = buffer.getLong();
        long lastApiFetchTime = buffer.getLong();
        long intervalMillis = buffer.getLong();

        int coinLen = Byte.toUnsignedInt(buffer.get());
        if (buffer.remaining() < coinLen) {
            throw new IllegalArgumentException("Buffer truncated while reading coin name (need " + coinLen + " bytes)");
        }
        byte[] coinBytes = new byte[coinLen];
        buffer.get(coinBytes);
        String coin = new String(coinBytes, StandardCharsets.UTF_8);

        if (buffer.hasRemaining()) {
            buffer.mark();
            int exchangeLen = Byte.toUnsignedInt(buffer.get());
            if (exchangeLen <= buffer.remaining() && exchangeLen > 0) {
                byte[] exchangeBytes = new byte[exchangeLen];
                buffer.get(exchangeBytes);
                String exchangeStr = new String(exchangeBytes, StandardCharsets.UTF_8);
                if ("binance".equalsIgnoreCase(exchangeStr)) {
                    int legacyHeaderSize = buffer.position() - startPos;
                    return new Metadata(firstKlineTime, lastApiFetchTime, intervalMillis, coin, legacyHeaderSize);
                }
            }
            buffer.reset();
        }

        int headerSize = buffer.position() - startPos;
        return new Metadata(firstKlineTime, lastApiFetchTime, intervalMillis, coin, headerSize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Metadata metadata)) return false;
        return firstKlineTime == metadata.firstKlineTime &&
                lastApiFetchTime == metadata.lastApiFetchTime &&
                intervalMillis == metadata.intervalMillis &&
                coin.equalsIgnoreCase(metadata.coin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstKlineTime, lastApiFetchTime, intervalMillis, coin.toLowerCase());
    }

    @Override
    public String toString() {
        return String.format(
                "Metadata[coin=%s, interval=%dms, firstKlineTime=%d, lastApiFetchTime=%d, size=%d bytes]",
                coin, intervalMillis, firstKlineTime, lastApiFetchTime, size()
        );
    }
}
