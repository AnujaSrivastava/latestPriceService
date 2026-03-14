package com.interview.prices;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LatestValuePriceServiceJUnitTest {

    @Test
    void completedBatchPublishesAtomically() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId batchId = service.beginPriceBatch();

        service.addPricesToBatch(batchId, List.of(price("A", "2026-01-01T10:15:30Z", 100)));

        assertFalse(service.getLatestPrice("A").isPresent());

        service.commitBatch(batchId);

        assertEquals(100, service.getLatestPrice("A").orElseThrow().payload().get("value"));
    }

    @Test
    void lastValueUsesAsOfRatherThanArrivalOrder() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId firstBatch = service.beginPriceBatch();
        service.addPricesToBatch(firstBatch, List.of(price("A", "2026-01-01T10:15:30Z", 110)));
        service.commitBatch(firstBatch);

        LatestValuePriceService.BatchId secondBatch = service.beginPriceBatch();
        service.addPricesToBatch(secondBatch, List.of(price("A", "2025-12-31T23:59:59Z", 90)));
        service.commitBatch(secondBatch);

        assertEquals(110, service.getLatestPrice("A").orElseThrow().payload().get("value"));
    }

    @Test
    void parallelChunkUploadsProduceLatestPerId() throws Exception {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId batchId = service.beginPriceBatch();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch ready = new CountDownLatch(4);
        CountDownLatch start = new CountDownLatch(1);

        try {
            List<Callable<Void>> tasks = List.of(
                    uploadTask(batchId, ready, start, service, price("A", "2026-01-01T10:00:00Z", 100)),
                    uploadTask(batchId, ready, start, service, price("A", "2026-01-01T10:01:00Z", 101)),
                    uploadTask(batchId, ready, start, service, price("B", "2026-01-01T10:00:00Z", 200)),
                    uploadTask(batchId, ready, start, service, price("B", "2026-01-01T09:59:00Z", 199)));

            List<Future<Void>> futures = tasks.stream().map(executor::submit).toList();
            assertTrue(ready.await(2, TimeUnit.SECONDS));
            start.countDown();
            for (Future<Void> future : futures) {
                future.get(2, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        service.commitBatch(batchId);

        Map<String, LatestValuePriceService.PriceRecord> prices = service.getLatestPrices(List.of("A", "B"));
        assertEquals(101, prices.get("A").payload().get("value"));
        assertEquals(200, prices.get("B").payload().get("value"));
    }

    @Test
    void invalidBatchLifecycleCallsAreRejected() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId batchId = service.beginPriceBatch();
        service.commitBatch(batchId);

        assertThrows(IllegalStateException.class,
                () -> service.addPricesToBatch(batchId, List.of(price("A", "2026-01-01T10:15:30Z", 100))));
        assertThrows(IllegalStateException.class, () -> service.commitBatch(batchId));
        assertThrows(IllegalStateException.class, () -> service.abortBatch(batchId));
    }

    @Test
    void returnedLatestPricesSnapshotIsImmutable() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId batchId = service.beginPriceBatch();
        service.addPricesToBatch(batchId, List.of(price("A", "2026-01-01T10:15:30Z", 100)));
        service.commitBatch(batchId);

        Map<String, LatestValuePriceService.PriceRecord> snapshot = service.getLatestPrices(List.of("A"));

        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.put("B", price("B", "2026-01-01T10:16:30Z", 200)));
    }

    private Callable<Void> uploadTask(
            LatestValuePriceService.BatchId batchId,
            CountDownLatch ready,
            CountDownLatch start,
            LatestValuePriceService service,
            LatestValuePriceService.PriceRecord record) {
        return () -> {
            ready.countDown();
            assertTrue(start.await(2, TimeUnit.SECONDS));
            service.addPricesToBatch(batchId, List.of(record));
            return null;
        };
    }

    private LatestValuePriceService.PriceRecord price(String id, String asOf, int value) {
        return new LatestValuePriceService.PriceRecord(
                id,
                Instant.parse(asOf),
                Map.of("value", value, "ccy", "USD"));
    }
}
