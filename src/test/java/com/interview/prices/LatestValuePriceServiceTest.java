package com.interview.prices;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * Manual test class for LatestValuePriceService.
 *
 * This class contains a set of test methods that verify the correctness and thread-safety
 * of the LatestValuePriceService class. Each test simulates a different scenario
 * and uses custom assertion helpers. To run all tests, execute the main method.
 *
 * Note: This is a manual test class, not a JUnit test. For production code, prefer JUnit 5.
 */
public final class LatestValuePriceServiceTest {

    /**
     * Runs all test methods in this class.
     */
    public static void main(String[] args) throws Exception {
        LatestValuePriceServiceTest test = new LatestValuePriceServiceTest();
        test.completedBatchPublishesAtomically();
        test.lastValueIsBasedOnAsOfAndNotArrivalOrder();
        test.parallelChunkUploadsProduceLatestPerId();
        test.cancelledBatchIsDiscarded();
        test.invalidBatchLifecycleCallsAreRejected();
        test.consumersOnlyReceiveRequestedIdsThatExist();
        test.returnedLatestPricesSnapshotIsImmutable();
        System.out.println("All tests passed.");
    }

    /**
     * Test: A batch is not visible until it is completed, then all its prices become visible atomically.
     */
    void completedBatchPublishesAtomically() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId batchId = service.beginPriceBatch();

        service.addPricesToBatch(batchId, List.of(price("A", "2026-01-01T10:15:30Z", 100)));
        assertFalse(service.getLatestPrice("A").isPresent(), "Incomplete batch must stay invisible");

        service.commitBatch(batchId);

        LatestValuePriceService.PriceRecord published = service.getLatestPrice("A").orElseThrow();
        assertEquals(100, published.payload().get("value"), "Completed batch should publish value");
    }

    /**
     * Test: The latest value is determined by the 'asOf' timestamp, not by the order of arrival.
     */
    void lastValueIsBasedOnAsOfAndNotArrivalOrder() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId firstBatch = service.beginPriceBatch();
        service.addPricesToBatch(firstBatch, List.of(price("A", "2026-01-01T10:15:30Z", 110)));
        service.commitBatch(firstBatch);

        LatestValuePriceService.BatchId secondBatch = service.beginPriceBatch();
        service.addPricesToBatch(secondBatch, List.of(price("A", "2025-12-31T23:59:59Z", 90)));
        service.commitBatch(secondBatch);

        LatestValuePriceService.PriceRecord published = service.getLatestPrice("A").orElseThrow();
        assertEquals(110, published.payload().get("value"), "Newer asOf should win");
    }

    /**
     * Test: Parallel uploads in a batch produce the latest value per ID, based on timestamp.
     */
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
            assertTrue(ready.await(2, TimeUnit.SECONDS), "Workers must be ready before release");
            start.countDown();
            for (Future<Void> future : futures) {
                future.get(2, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        service.commitBatch(batchId);

        Map<String, LatestValuePriceService.PriceRecord> prices = service.getLatestPrices(List.of("A", "B"));
        assertEquals(101, prices.get("A").payload().get("value"), "Latest A price should win");
        assertEquals(200, prices.get("B").payload().get("value"), "Latest B price should win");
    }

    /**
     * Test: Cancelling a batch discards all its uploaded prices and makes them invisible.
     */
    void cancelledBatchIsDiscarded() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId batchId = service.beginPriceBatch();
        service.addPricesToBatch(batchId, List.of(price("A", "2026-01-01T10:15:30Z", 100)));

        service.abortBatch(batchId);

        assertFalse(service.getLatestPrice("A").isPresent(), "Cancelled batch should be discarded");
    }

    /**
     * Test: Invalid lifecycle operations (upload/commit/cancel after commit) are rejected with exceptions.
     */
    void invalidBatchLifecycleCallsAreRejected() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId batchId = service.beginPriceBatch();
        service.commitBatch(batchId);

        assertThrows(IllegalStateException.class,
                () -> service.addPricesToBatch(batchId, List.of(price("A", "2026-01-01T10:15:30Z", 100))),
                "Uploading to a completed batch must fail");
        assertThrows(IllegalStateException.class, () -> service.commitBatch(batchId),
                "Completing a completed batch must fail");
        assertThrows(IllegalStateException.class, () -> service.abortBatch(batchId),
                "Cancelling a completed batch must fail");
    }

    /**
     * Test: Only requested IDs that exist are returned; missing IDs are not included in the result.
     */
    void consumersOnlyReceiveRequestedIdsThatExist() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId batchId = service.beginPriceBatch();
        service.addPricesToBatch(batchId, List.of(
                price("A", "2026-01-01T10:15:30Z", 100),
                price("B", "2026-01-01T10:15:31Z", 200)));
        service.commitBatch(batchId);

        Map<String, LatestValuePriceService.PriceRecord> prices = service.getLatestPrices(List.of("B", "C"));

        assertEquals(1, prices.size(), "Only existing ids should be returned");
        assertNotNull(prices.get("B"), "Existing requested id should be present");
        assertFalse(prices.containsKey("C"), "Missing id should not be returned");
    }

    /**
     * Test: Consumers receive an immutable snapshot and cannot alter committed prices through it.
     *
     * Note: preventing same-JVM reflective access to private fields is not something this service can
     * guarantee in a plain Java runtime. The enforceable boundary here is that the exposed snapshot is
     * immutable.
     */
    void returnedLatestPricesSnapshotIsImmutable() {
        LatestValuePriceService service = new LatestValuePriceService();
        LatestValuePriceService.BatchId batchId = service.beginPriceBatch();
        service.addPricesToBatch(batchId, List.of(price("A", "2026-01-01T10:15:30Z", 100)));
        service.commitBatch(batchId);

        Map<String, LatestValuePriceService.PriceRecord> snapshot = service.getLatestPrices(List.of("A"));

        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.put("B", price("B", "2026-01-01T10:16:30Z", 200)),
                "Returned latest-prices snapshot must be immutable");
    }

    /**
     * Helper to create a parallel upload task for a price record in a batch.
     * Used to simulate concurrent uploads in a batch.
     */
    private Callable<Void> uploadTask(
            LatestValuePriceService.BatchId batchId,
            CountDownLatch ready,
            CountDownLatch start,
            LatestValuePriceService service,
            LatestValuePriceService.PriceRecord record) {
        return () -> {
            ready.countDown();
            assertTrue(start.await(2, TimeUnit.SECONDS), "Workers must be released together");
            service.addPricesToBatch(batchId, List.of(record));
            return null;
        };
    }

    /**
     * Helper to create a PriceRecord for testing.
     * @param id the price ID
     * @param asOf the ISO-8601 timestamp string
     * @param value the price value
     * @return a new PriceRecord
     */
    private LatestValuePriceService.PriceRecord price(String id, String asOf, int value) {
        return new LatestValuePriceService.PriceRecord(
                id,
                Instant.parse(asOf),
                Map.of("value", value, "ccy", "USD"));
    }

    // --- Assertion helpers for manual test style ---

    /**
     * Asserts that a condition is true, otherwise throws AssertionError with the given message.
     */
    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Asserts that a condition is false, otherwise throws AssertionError with the given message.
     */
    private static void assertFalse(boolean condition, String message) {
        assertTrue(!condition, message);
    }

    /**
     * Asserts that a value is not null, otherwise throws AssertionError with the given message.
     */
    private static void assertNotNull(Object value, String message) {
        assertTrue(value != null, message);
    }

    /**
     * Asserts that two values are equal, otherwise throws AssertionError with the given message.
     */
    private static void assertEquals(Object expected, Object actual, String message) {
        if (!java.util.Objects.equals(expected, actual)) {
            throw new AssertionError(message + ": expected=" + expected + ", actual=" + actual);
        }
    }

    /**
     * Asserts that a runnable throws the expected exception type, otherwise throws AssertionError.
     */
    private static <T extends Throwable> void assertThrows(
            Class<T> expectedType, ThrowingRunnable runnable, String message) {
        try {
            runnable.run();
        } catch (Throwable throwable) {
            if (expectedType.isInstance(throwable)) {
                return;
            }
            throw new AssertionError(message + ": unexpected exception " + throwable, throwable);
        }
        throw new AssertionError(message + ": expected exception " + expectedType.getSimpleName());
    }

    /**
     * Functional interface for lambdas that throw checked exceptions.
     */
    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
