package com.interview.prices;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory service for publishing and reading latest prices.
 *
 * <p>The public API is intentionally kept in a single class as requested by the assignment. A batch
 * is isolated until completion, at which point its prices become visible in one atomic snapshot
 * update.
 */
public final class LatestValuePriceService {

    private final AtomicReference<Map<String, PriceRecord>> latestCommittedPrices =
            new AtomicReference<>(Map.of());
    private final ConcurrentMap<BatchId, BatchProcess> activeBatches = new ConcurrentHashMap<>();
    private final Object commitLock = new Object();


    /**
     * Starts a new batch for uploading prices.
     * @return a unique BatchId for the new batch
     */
    public BatchId beginPriceBatch() {
        BatchId batchId = new BatchId(UUID.randomUUID());
        BatchProcess previous = activeBatches.putIfAbsent(batchId, new BatchProcess());
        if (previous != null) {
            throw new IllegalStateException("Duplicate batch id generated");
        }
        return batchId;
    }

    /**
     * Uploads a collection of price records to an active batch.
     * @param batchId the batch to upload to
     * @param priceRecords the price records to upload
     */
    public void addPricesToBatch(BatchId batchId, Collection<PriceRecord> priceRecords) {
        Objects.requireNonNull(batchId, "batchId must not be null");
        Objects.requireNonNull(priceRecords, "priceRecords must not be null");
        getActiveBatchProcess(batchId).addPriceRecords(priceRecords);
    }

    /**
     * Completes a batch, atomically making its prices visible to all readers.
     * @param batchId the batch to complete
     */
    public void commitBatch(BatchId batchId) {
        Objects.requireNonNull(batchId, "batchId must not be null");
        BatchProcess batch = getActiveBatchProcess(batchId);
        Map<String, PriceRecord> batchPrices = batch.completeAndGetSnapshot();
        synchronized (commitLock) {
            Map<String, PriceRecord> current = latestCommittedPrices.get();
            Map<String, PriceRecord> merged = new HashMap<>(current);
            batchPrices.forEach((id, candidate) ->
                    merged.merge(id, candidate, LatestValuePriceService::chooseLatestByTimestamp));
            latestCommittedPrices.set(Map.copyOf(merged));
        }
        activeBatches.remove(batchId, batch);
    }

    /**
     * Cancels an active batch, discarding its data.
     * @param batchId the batch to cancel
     */
    public void abortBatch(BatchId batchId) {
        Objects.requireNonNull(batchId, "batchId must not be null");
        BatchProcess batch = getActiveBatchProcess(batchId);
        batch.cancel();
        activeBatches.remove(batchId, batch);
    }

    /**
     * Retrieves the latest prices for a collection of IDs.
     * @param ids the IDs to look up
     * @return a map of ID to latest PriceRecord
     */
    public Map<String, PriceRecord> getLatestPrices(Collection<String> ids) {
        Objects.requireNonNull(ids, "ids must not be null");
        Map<String, PriceRecord> snapshot = latestCommittedPrices.get();
        Map<String, PriceRecord> result = new LinkedHashMap<>();
        for (String id : ids) {
            Objects.requireNonNull(id, "ids must not contain null values");
            PriceRecord record = snapshot.get(id);
            if (record != null) {
                result.put(id, record);
            }
        }
        return Map.copyOf(result);
    }

    /**
     * Retrieves the latest price for a single ID.
     * @param id the ID to look up
     * @return an Optional containing the latest PriceRecord, if present
     */
    public Optional<PriceRecord> getLatestPrice(String id) {
        Objects.requireNonNull(id, "id must not be null");
        return Optional.ofNullable(latestCommittedPrices.get().get(id));
    }

    private BatchProcess getActiveBatchProcess(BatchId batchId) {
        BatchProcess batch = activeBatches.get(batchId);
        if (batch == null) {
            throw new IllegalStateException("Batch " + batchId.value() + " is not active");
        }
        return batch;
    }

    private static PriceRecord chooseLatestByTimestamp(PriceRecord a, PriceRecord b) {
        return b.asOf().isAfter(a.asOf()) ? b : a;
    }

    /**
     * Unique identifier for a batch process.
     */
    public record BatchId(UUID value) {
        public BatchId {
            Objects.requireNonNull(value, "value must not be null");
        }
    }

    /**
     * Represents a price record with an ID, timestamp, and payload.
     */
    public record PriceRecord(String id, Instant asOf, Map<String, Object> payload) {
        public PriceRecord {
            Objects.requireNonNull(id, "id must not be null");
            Objects.requireNonNull(asOf, "asOf must not be null");
            Objects.requireNonNull(payload, "payload must not be null");
            payload = Map.copyOf(payload);
        }
    }

    private static final class BatchProcess {
        private final ConcurrentMap<String, PriceRecord> pricesInBatch = new ConcurrentHashMap<>();
        private final AtomicReference<BatchStatus> status = new AtomicReference<>(BatchStatus.OPEN);
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        void addPriceRecords(Collection<PriceRecord> records) {
            lock.readLock().lock();
            try {
                ensureOpen();
                for (PriceRecord record : records) {
                    Objects.requireNonNull(record, "records must not contain null values");
                    pricesInBatch.merge(record.id(), record, LatestValuePriceService::chooseLatestByTimestamp);
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        Map<String, PriceRecord> completeAndGetSnapshot() {
            lock.writeLock().lock();
            try {
                if (!status.compareAndSet(BatchStatus.OPEN, BatchStatus.COMPLETED)) {
                    throw new IllegalStateException("Batch is not open");
                }
                return Map.copyOf(pricesInBatch);
            } finally {
                lock.writeLock().unlock();
            }
        }

        void cancel() {
            lock.writeLock().lock();
            try {
                if (!status.compareAndSet(BatchStatus.OPEN, BatchStatus.CANCELLED)) {
                    throw new IllegalStateException("Batch is not open");
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        private void ensureOpen() {
            if (status.get() != BatchStatus.OPEN) {
                throw new IllegalStateException("Batch is not open");
            }
        }
    }

    private enum BatchStatus {
        OPEN,
        COMPLETED,
        CANCELLED
    }
}
