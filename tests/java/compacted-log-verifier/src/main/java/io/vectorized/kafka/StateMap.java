package io.vectorized.kafka;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateMap implements Serializable {
  static FSTConfiguration fst = FSTConfiguration.createDefaultConfiguration();
  static final Logger logger = LoggerFactory.getLogger(StateMap.class);
  private Map<Long, KeyStats> keys = new HashMap<>();
  private long totalRecords = 0;

  public static StateMap readFrom(Path path) throws IOException {
    byte[] content = Files.readAllBytes(path);
    return (StateMap)fst.asObject(content);
  }

  public static boolean compareStates(StateMap ref, StateMap have) {
    long refUpdates = 0;
    long consumedUpdates = 0;
    boolean success = true;
    for (Map.Entry<Long, KeyStats> e : ref.keys.entrySet()) {
      final KeyStats consumedKeyStat = have.keys.get(e.getKey());
      final KeyStats refKeyStat = e.getValue();
      if (consumedKeyStat == null) {
        logger.warn(
            "Key {} exists in reference state but it does not in recovered state",
            e.getKey());
        success = false;
        continue;
      }

      if (consumedKeyStat.latestValue != refKeyStat.latestValue) {
        logger.warn(
            "Key {} values are different. Expected value {} != recovered value {}",
            e.getKey(), refKeyStat.latestValue, consumedKeyStat.latestValue);
        success = false;
      }

      if (consumedKeyStat.latestValue != refKeyStat.latestValue) {
        logger.warn(
            "Key {} value offsets are different. Expected value offset {} != recovered value offset {}",
            e.getKey(), refKeyStat.latestValueOffset,
            consumedKeyStat.latestValueOffset);
        success = false;
      }

      refUpdates += refKeyStat.updates;
      consumedUpdates += consumedKeyStat.updates;
    }
    logger.info("Reference state has {} distinct keys", ref.keys.size());
    logger.info("Consumed state has {} distinct keys", have.keys.size());
    logger.info(
        "Produced records {}, consumed records {}", refUpdates,
        consumedUpdates);
    return success;
  }

  public void updateRecord(byte[] key, byte[] value, long offset) {
    long v = ByteBuffer.wrap(value).rewind().getLong();
    totalRecords++;
    keys.compute(ByteBuffer.wrap(key).rewind().getLong(), (k, stats) -> {
      if (stats == null) {
        stats = new KeyStats();
      }
      stats.latestValue = v;
      stats.latestValueOffset = offset;
      stats.updates++;
      return stats;
    });
  }

  public long getTotalRecords() { return totalRecords; }

  public void write(Path path) throws IOException {
    Files.write(path, fst.asByteArray(this));
  }

  @Override
  public String toString() {
    return "StateMap{"
        + "keys=" + keys + '}';
  }
}
