package com.lukaulcar.storage;

import com.lukaulcar.model.Kline;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Memory-efficient, chunked iterator for sequential disk reads of {@link Kline} records.
 * <p>
 * Holds a read lock on the underlying storage file for the duration of the iteration.
 * <b>Important:</b> Always use within a {@code try-with-resources} statement to guarantee
 * that the read lock is released and the file channel is closed.
 */
public class KlineIterator implements Iterator<Kline>, Closeable, AutoCloseable {

    private final FileChannel channel;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ByteBuffer buffer;
    private final long endFilePos;
    private long currentFilePos;
    private Kline nextKline;
    private boolean closed;

    /**
     * Constructs a KlineIterator over a specific byte range in a storage file.
     *
     * @param channel        file channel to read from
     * @param readLock       read lock protecting the file
     * @param startFilePos   starting byte offset
     * @param endFilePos     ending byte offset
     * @param bufferCapacity capacity in bytes for each chunk buffer
     */
    public KlineIterator(FileChannel channel,
                         ReentrantReadWriteLock.ReadLock readLock,
                         long startFilePos,
                         long endFilePos,
                         int bufferCapacity) {
        this.channel = channel;
        this.readLock = readLock;
        this.currentFilePos = startFilePos;
        this.endFilePos = endFilePos;
        int size = (bufferCapacity / Kline.BYTES) * Kline.BYTES;
        if (size < Kline.BYTES) {
            size = Kline.BYTES;
        }
        this.buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        this.buffer.limit(0);
    }

    /**
     * Checks if there are more klines available to read.
     *
     * @return true if the iteration has more klines
     */
    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        if (nextKline != null) {
            return true;
        }
        advance();
        return nextKline != null;
    }

    /**
     * Returns the next {@link Kline} in the iteration.
     *
     * @return the next kline
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Kline next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Kline k = nextKline;
        nextKline = null;
        return k;
    }

    private void advance() {
        while (nextKline == null && !closed) {
            while (buffer.remaining() >= Kline.BYTES) {
                Kline candidate = Kline.readFrom(buffer);
                if (candidate.isValid()) {
                    nextKline = candidate;
                    return;
                }
            }

            long bytesRemaining = endFilePos - currentFilePos;
            if (bytesRemaining <= 0) {
                close();
                return;
            }

            int toRead = (int) Math.min(bytesRemaining, buffer.capacity());
            buffer.clear();
            buffer.limit(toRead);

            while (buffer.hasRemaining()) {
                try {
                    int bytesRead = channel.read(buffer, currentFilePos + buffer.position());
                    if (bytesRead <= 0) {
                        break;
                    }
                } catch (IOException e) {
                    close();
                    throw new RuntimeException("Failed reading klines from disk", e);
                }
            }

            int readCount = buffer.position();
            if (readCount == 0) {
                close();
                return;
            }

            currentFilePos += readCount;
            buffer.flip();
        }
    }

    /**
     * Closes this iterator, releases the file read lock, and closes the underlying {@link FileChannel}.
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (channel != null) {
                    channel.close();
                }
            } catch (IOException ignored) {
            } finally {
                if (readLock != null) {
                    readLock.unlock();
                }
            }
        }
    }
}
