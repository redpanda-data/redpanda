package io.vectorized.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public final class VerifierCallback implements Callback {
  private final long start;
  private final int iteration;
  private final int bytes;
  private final ProducerStats stats;
  private final long recordTs;
  private final long recordCnt;

  public VerifierCallback(
      int iter, long start, int bytes, ProducerStats stats, long recordTs,
      long recordCnt) {
    this.start = start;
    this.stats = stats;
    this.iteration = iter;
    this.bytes = bytes;
    this.recordTs = recordTs;
    this.recordCnt = recordCnt;
  }

  public void onCompletion(RecordMetadata metadata, Exception exception) {
    long now = System.currentTimeMillis();
    int latency = (int)(now - start);
    this.stats.record(
        iteration, latency, bytes, now, metadata, recordTs, recordCnt);

    if (exception != null) {
      exception.printStackTrace();
    }
  }
}
