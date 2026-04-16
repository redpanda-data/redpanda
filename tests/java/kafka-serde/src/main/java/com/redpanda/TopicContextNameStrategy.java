/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.redpanda;

import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import java.util.Map;

/**
 * A configurable ContextNameStrategy that returns a fixed context name.
 * The context name is read from the config key
 * "context.name.strategy.context.name".
 */
public class TopicContextNameStrategy implements ContextNameStrategy {
  public static final String CONTEXT_NAME_CONFIG
      = "context.name.strategy.context.name";

  private String contextName;

  @Override
  public void configure(Map<String, ?> config) {
    this.contextName = (String)config.get(CONTEXT_NAME_CONFIG);
    if (this.contextName == null) {
      throw new IllegalArgumentException(
          CONTEXT_NAME_CONFIG
          + " must be set when using TopicContextNameStrategy");
    }
  }

  @Override
  public String contextName(String topic) {
    return contextName;
  }
}
