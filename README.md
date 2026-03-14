# Latest Value Price Service

This project implements an in-memory Java service for publishing batches of price records and
serving the latest committed value per instrument.

## Highlights

- Producers work with explicit batch lifecycles: `startBatch`, `uploadChunk`, `completeBatch`,
  `cancelBatch`
- Chunks can be uploaded in parallel for the same batch
- Consumers only read from the latest committed snapshot, so incomplete batches never leak
- "Last value" is chosen by the producer-supplied `asOf` timestamp, not by arrival order

## Run tests

```bash
mvn test
```

If Maven cannot download plugins in a restricted environment, the same test suite can be run directly
with the JDK:

```bash
mkdir -p /tmp/latest-value-price-service-classes
javac --release 21 -d /tmp/latest-value-price-service-classes $(find src/main/java src/test/java -name '*.java')
java -cp /tmp/latest-value-price-service-classes com.interview.prices.LatestValuePriceServiceTest
```
