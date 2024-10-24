package io.vectorized.compaction;
import java.util.ArrayList;
import java.util.Random;

public abstract class GatedWorkload {
  public static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  public static String randomString(Random random, int length) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < length; i++) {
      buf.append(LETTERS.charAt(random.nextInt(LETTERS.length())));
    }
    return buf.toString();
  }

  protected static enum State {
    INITIAL,
    PRODUCING,
    DRAINING,
    DRAINED,
    CONSUMING,
    FINAL
  }

  protected volatile State state = State.INITIAL;
  protected volatile long started_s;
  protected long count = 0;

  protected ArrayList<Thread> write_threads;
  protected ArrayList<Thread> read_threads;

  public void startProducer() throws Exception {
    synchronized (this) {
      if (this.state != State.INITIAL) {
        violation(-1, "producer can be started only from the initial state");
      }

      this.state = State.PRODUCING;
    }

    this.started_s = System.currentTimeMillis() / 1000;
  }

  public void stopProducer() throws Exception {
    synchronized (this) {
      if (this.state != State.PRODUCING) {
        violation(-1, "producer can be stopped only from the producing state");
      }

      this.state = State.DRAINING;
    }

    log(-1, "producer's stop requested");
  }

  public void waitProducer() throws Exception {
    if (this.state != State.DRAINED && this.state != State.DRAINING) {
      violation(-1, "producer isn't active");
    }

    if (this.state == State.DRAINED) {
      return;
    }

    for (var th : write_threads) {
      th.join();
    }
    log(-1, "writers stopped");

    this.state = State.DRAINED;
  }

  public void startConsumer() throws Exception {
    synchronized (this) {
      if (this.state != State.DRAINED) {
        violation(-1, "consumer can be started only from the initial state");
      }
      this.state = State.CONSUMING;
    }
  }

  public void waitConsumer() throws Exception {
    if (this.state == State.FINAL) {
      return;
    } else if (this.state != State.CONSUMING) {
      violation(-1, "can't wait consumer before starting it");
    }

    for (var th : read_threads) {
      th.join();
    }

    this.state = State.FINAL;
  }

  public abstract App.Metrics getMetrics();

  protected synchronized void violation(int partition, String msg) {
    long ts = System.currentTimeMillis() / 1000 - started_s;
    System.out.println(
        "" + (count++) + "\t" + ts + "\t" + partition + "\tviolation\t" + msg);
    System.exit(1);
  }

  protected synchronized void log(int partition, String msg) {
    long ts = System.currentTimeMillis() / 1000 - started_s;
    System.out.println(
        "" + (count++) + "\t" + ts + "\t" + partition + "\t" + msg);
  }
}
