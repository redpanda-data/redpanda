package io.vectorized.kafka;

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stats {
  private static final Logger logger = LoggerFactory.getLogger(Stats.class);
  private long start;
  private long windowStart;
  private int[] latencies;
  private int sampling;
  private int index = 0;
  private long count = 0;
  private long bytes = 0;
  private int maxLatency = 0;
  private long totalLatency = 0;
  private long windowCount = 0;
  private int windowMaxLatency = 0;
  private long windowTotalLatency = 0;
  private long windowBytes = 0;
  private long reportingInterval;
  private final String prefix;

  public Stats(long numRecords, int reportingInterval, String prefix) {
    this.start = System.currentTimeMillis();
    this.windowStart = System.currentTimeMillis();
    this.sampling = (int)(numRecords / Math.min(numRecords, 500000));
    this.latencies = new int[(int)(numRecords / this.sampling) + 1];
    this.reportingInterval = reportingInterval;
    this.prefix = prefix;
  }

  public void record(long iter, int latency, int bytes, long time) {
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
    long elapsed = System.currentTimeMillis() - windowStart;
    double recsPerSec = 1000.0 * windowCount / (double)elapsed;
    double mbPerSec
        = 1000.0 * this.windowBytes / (double)elapsed / (1024.0 * 1024.0);
    logger.info(
        "{} - {} records {} records/sec ({} MB/sec), {} ms avg latency, {} ms max latency.",

        prefix, windowCount, String.format("%.1f", recsPerSec),
        String.format("%.2f", mbPerSec),
        String.format("%.1f", windowTotalLatency / (double)windowCount),
        String.format("%.1f", (double)windowMaxLatency));
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
    logger.info(
        "{} - {} records, {} records/sec ({} MB/sec), {} ms avg latency, "
            + "{} ms max latency, {} ms 50th, {} ms 95th, {} ms 99th, {} ms 99.9th.",
        prefix, count, String.format("%.1f", recsPerSec),
        String.format("%.2f", mbPerSec),
        String.format("%.2f", totalLatency / (double)count),
        String.format("%.2f", (double)maxLatency), percs[0], percs[1], percs[2],
        percs[3]);
  }

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
