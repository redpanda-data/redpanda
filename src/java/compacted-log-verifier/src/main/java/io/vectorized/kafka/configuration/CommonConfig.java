package io.vectorized.kafka.configuration;

import java.util.Optional;
import java.util.Properties;
import org.immutables.value.Value;

public interface CommonConfig {
  String brokers();
  String topic();
  Optional<String> statePath();
  Optional<Properties> securityProperties();
}
