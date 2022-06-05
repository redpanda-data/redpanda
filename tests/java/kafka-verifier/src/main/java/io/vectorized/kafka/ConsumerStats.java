package io.vectorized.kafka;

import java.util.Arrays;
import org.apache.kafka.clients.producer.Callback;

public class ConsumerStats {
  private long start;
  private long windowStart;
  private int[] latencies;
  private int sampling;
  private int iteration;
  private int index;
  private long count;
  private long bytes;
  private int maxLatency;
  private long totalLatency;
  private long windowCount;
  private int windowMaxLatency;
  private long windowTotalLatency;
  private long windowBytes;
  private long reportingInterval;
  private long maxRecords;

  public ConsumerStats(long numRecords, int reportingInterval) {
    this.start = System.currentTimeMillis();
    this.windowStart = System.currentTimeMillis();
    this.iteration = 0;
    this.sampling = (int)(numRecords / Math.min(numRecords, 500000));
    this.latencies = new int[(int)(numRecords / this.sampling) + 1];
    this.index = 0;
    this.maxLatency = 0;
    this.totalLatency = 0;
    this.windowCount = 0;
    this.windowMaxLatency = 0;
    this.windowTotalLatency = 0;
    this.windowBytes = 0;
    this.totalLatency = 0;
    this.reportingInterval = reportingInterval;
    this.maxRecords = numRecords;
  }

  public void record(final int iter, final int latency, final int bytes, final long time) {
    this.count++;
    this.bytes += bytes;
    this.totalLatency += latency;
    this.maxLatency = Math.max(this.maxLatency, latency);
    this.windowCount++;
    this.windowBytes += bytes;
    this.windowTotalLatency += latency;
    this.windowMaxLatency = Math.max(windowMaxLatency, latency);
    if (iter % this.sampling == 0) {
      this.latencies[index] = latency;
      this.index++;
    }
    /* maybe report the recent perf */
    if (time - windowStart >= reportingInterval) {
      printWindow();
      newWindow();
    }
  }

  public void printWindow() {
    long ellapsed = System.currentTimeMillis() - windowStart;
    double recsPerSec = 1000.0 * windowCount / (double)ellapsed;
    double mbPerSec
        = 1000.0 * this.windowBytes / (double)ellapsed / (1024.0 * 1024.0);
    System.out.printf(
        "CONS - %d records received, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
        windowCount, recsPerSec, mbPerSec,
        windowTotalLatency / (double)windowCount, (double)windowMaxLatency);
  }

  public void newWindow() {
    this.windowStart = System.currentTimeMillis();
    this.windowCount = 0;
    this.windowMaxLatency = 0;
    this.windowTotalLatency = 0;
    this.windowBytes = 0;
  }

  public void printTotal() {
    long elapsed = System.currentTimeMillis() - start;
    double recsPerSec = 1000.0 * count / (double)elapsed;
    double mbPerSec = 1000.0 * this.bytes / (double)elapsed / (1024.0 * 1024.0);
    int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
    System.out.printf(
        "%d records received, %f records/sec (%.2f MB/sec), %.2f ms avg latency,"
            + " %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
        count, recsPerSec, mbPerSec, totalLatency / (double)count,
        (double)maxLatency, percs[0], percs[1], percs[2], percs[3]);
  }

  boolean finished() { return this.count >= maxRecords; }

  private static int[] percentiles(
      int[] latencies, int count, double... percentiles) {
    int size = Math.min(count, latencies.length);
    Arrays.sort(latencies, 0, size);
    int[] values = new int[percentiles.length];
    for (int i = 0; i < percentiles.length; i++) {
      int index = (int)(percentiles[i] * size);
      values[i] = latencies[index];
    }
    return values;
  }
}
