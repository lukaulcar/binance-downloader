package com.lukaulcar.model;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Represents a single candlestick (kline) with fixed 48-byte binary serialization.
 *
 * @param open      the opening price
 * @param high      the highest price
 * @param low       the lowest price
 * @param close     the closing price
 * @param volume    the base asset volume
 * @param closeTime the candle close timestamp in epoch milliseconds (normalized)
 */
public record Kline(
        double open,
        double high,
        double low,
        double close,
        double volume,
        long closeTime
) {
    /**
     * Fixed binary size per Kline record in bytes: 5 doubles (5*8=40) + 1 long (8) = 48 bytes.
     */
    public static final int BYTES = 48;

    /**
     * Computes the candle opening timestamp in milliseconds given the interval duration.
     *
     * @param intervalMillis duration of the candle in milliseconds
     * @return the open timestamp (epoch millis)
     */
    public long openTime(long intervalMillis) {
        return closeTime - intervalMillis;
    }

    /**
     * Computes the candle opening timestamp in milliseconds given the interval.
     *
     * @param interval the interval representing the candle timeframe
     * @return the open timestamp (epoch millis)
     */
    public long openTime(Interval interval) {
        return openTime(interval.millis());
    }

    /**
     * Serializes this Kline into a newly allocated 48-byte array (Little Endian).
     *
     * @return 48-byte array containing the binary representation of this kline
     */
    public byte[] toBytes() {
        byte[] bytes = new byte[BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        writeTo(buffer);
        return bytes;
    }

    /**
     * Writes this Kline's binary fields into the provided {@link ByteBuffer} in Little Endian order.
     *
     * @param buffer target byte buffer to write 48 bytes into
     */
    public void writeTo(ByteBuffer buffer) {
        buffer.putDouble(open);
        buffer.putDouble(high);
        buffer.putDouble(low);
        buffer.putDouble(close);
        buffer.putDouble(volume);
        buffer.putLong(closeTime);
    }

    /**
     * Deserializes a Kline from a byte array.
     *
     * @param data byte array containing at least 48 bytes
     * @return the deserialized {@link Kline}
     * @throws NullPointerException     if {@code data} is null
     * @throws IllegalArgumentException if {@code data.length} is less than 48 bytes
     */
    public static Kline fromBytes(byte[] data) {
        Objects.requireNonNull(data, "data array cannot be null");
        if (data.length < BYTES) {
            throw new IllegalArgumentException("Expected at least " + BYTES + " bytes, got " + data.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        return readFrom(buffer);
    }

    /**
     * Reads a Kline from the current position of the given {@link ByteBuffer} (Little Endian).
     *
     * @param buffer byte buffer with at least 48 bytes remaining
     * @return the parsed {@link Kline}
     */
    public static Kline readFrom(ByteBuffer buffer) {
        double open = buffer.getDouble();
        double high = buffer.getDouble();
        double low = buffer.getDouble();
        double close = buffer.getDouble();
        double volume = buffer.getDouble();
        long closeTime = buffer.getLong();
        return new Kline(open, high, low, close, volume, closeTime);
    }

    /**
     * Validates whether this record represents a real candle or an empty/uninitialized slot.
     * Unwritten slots on disk are zero-filled, resulting in {@code closeTime == 0}.
     *
     * @return true if the kline contains a valid non-zero close time
     */
    public boolean isValid() {
        return closeTime != 0;
    }

    /**
     * Returns a human-readable string representation of this Kline.
     *
     * @return formatted string showing timestamp and OHLCV values
     */
    @Override
    public String toString() {
        return String.format(
                "Kline[time=%d, O=%.4f, H=%.4f, L=%.4f, C=%.4f, V=%.4f]",
                closeTime, open, high, low, close, volume
        );
    }
}

