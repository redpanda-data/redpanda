package io.vectorized.kafka.configuration;

import java.util.List;
import java.util.Properties;
import org.immutables.value.Value;

@Value.Immutable
public interface ConsumerConfig extends CommonConfig {

  List<String> properties();

  String consumerGroup();

  default Properties parseProperties() {
    return Configuration.parseProperties(properties());
  }
}
