/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.redpanda;

import java.util.Properties;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Defines the security settings for the application
 */
public class SecuritySettings {
  private SecuritySettings() {}

  private String saslMechanism;
  private String saslPlainPassword;

  private String saslPlainUsername;

  private String securityProtocol;

  /**
   * Creates a Security settings from a JSONObject
   *
   * @param obj The object to parse
   * @return Instance of security settings
   *
   * @see org.json.JSONObject
   */
  public static SecuritySettings fromJSON(JSONObject obj) {
    SecuritySettings securitySettings = new SecuritySettings();
    try {
      securitySettings.saslMechanism = obj.getString("sasl_mechanism");
    } catch (JSONException ignored) {
    }

    try {
      securitySettings.securityProtocol = obj.getString("security_protocol");
    } catch (JSONException ignored) {
    }

    try {
      securitySettings.saslPlainUsername = obj.getString("sasl_plain_username");
    } catch (JSONException ignored) {
    }

    try {
      securitySettings.saslPlainPassword = obj.getString("sasl_plain_password");
    } catch (JSONException ignored) {
    }

    return securitySettings;
  }

  /**
   * @return A Properties instance containing the values of this class
   * @see java.util.Properties
   */
  public Properties toProperties() {
    Properties prop = new Properties();

    if (this.saslMechanism.contains("SCRAM")) {
      prop.put(
          "sasl.jaas.config",
          String.format(
              "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
              this.saslPlainUsername, this.saslPlainPassword));
    } else {
      throw new RuntimeException(
          String.format("Unknown sasl mechanism '%s'", this.saslMechanism));
    }

    prop.put("sasl.mechanism", this.saslMechanism);
    prop.put("security.protocol", this.securityProtocol);

    return prop;
  }

  /**
   * @return The properties as a string
   */
  public String toString() { return this.toProperties().toString(); }
}
