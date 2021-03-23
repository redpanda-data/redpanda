package io.vectorized.kafka.configuration;

import java.util.List;
import java.util.Properties;
import org.immutables.value.Value;

@Value.Immutable
public interface ProducerConfig extends CommonConfig {

  short replicationFactor();

  int partitions();

  int keyCardinality();

  int payloadSize();

  int keySize();

  List<String> properties();

  long recordsCount();

  int segmentSize();

  String compression();

  default Properties parseProperties() {
    return Configuration.parseProperties(properties());
  }
}
