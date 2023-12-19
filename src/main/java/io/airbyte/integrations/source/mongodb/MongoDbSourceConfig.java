/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb;

import static io.airbyte.integrations.source.mongodb.MongoConstants.AUTH_SOURCE_CONFIGURATION_KEY;
import static io.airbyte.integrations.source.mongodb.MongoConstants.CHECKPOINT_INTERVAL;
import static io.airbyte.integrations.source.mongodb.MongoConstants.CHECKPOINT_INTERVAL_CONFIGURATION_KEY;
import static io.airbyte.integrations.source.mongodb.MongoConstants.DATABASE_CONFIGURATION_KEY;
import static io.airbyte.integrations.source.mongodb.MongoConstants.DATABASE_CONFIG_CONFIGURATION_KEY;
import static io.airbyte.integrations.source.mongodb.MongoConstants.DEFAULT_AUTH_SOURCE;
import static io.airbyte.integrations.source.mongodb.MongoConstants.DEFAULT_DISCOVER_SAMPLE_SIZE;
import static io.airbyte.integrations.source.mongodb.MongoConstants.DISCOVER_SAMPLE_SIZE_CONFIGURATION_KEY;
import static io.airbyte.integrations.source.mongodb.MongoConstants.PASSWORD_CONFIGURATION_KEY;
import static io.airbyte.integrations.source.mongodb.MongoConstants.USERNAME_CONFIGURATION_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.OptionalInt;

/**
 * Represents the source's configuration, hiding the details of how the
 * underlying JSON
 * configuration is constructed.
 *
 * @param rawConfig The underlying JSON configuration provided by the connector
 *                  framework.
 */
public record MongoDbSourceConfig(JsonNode rawConfig) {

  private static String CERTIFICATE_CONFIGURATION_KEY = "cert";
  private static String PRIVATE_KEY_CONFIGURATION_KEY = "key";
  private static String CERTIFICATE_PASS_CONFIGURATION_KEY = "cert_pass";

  /**
   * Constructs a new {@link MongoDbSourceConfig} from the provided raw
   * configuration.
   *
   * @param rawConfig The underlying JSON configuration provided by the connector
   *                  framework.
   * @throws IllegalArgumentException if the raw configuration does not contain
   *                                  the
   *                                  {@link MongoConstants#DATABASE_CONFIG_CONFIGURATION_KEY}
   *                                  key.
   */
  public MongoDbSourceConfig(final JsonNode rawConfig) {
    if (rawConfig.has(DATABASE_CONFIG_CONFIGURATION_KEY)) {
      this.rawConfig = rawConfig.get(DATABASE_CONFIG_CONFIGURATION_KEY);
    } else {
      throw new IllegalArgumentException(
          "Database configuration is missing required '" + DATABASE_CONFIG_CONFIGURATION_KEY + "' property.");
    }
  }

  public String getAuthSource() {
    return rawConfig.has(AUTH_SOURCE_CONFIGURATION_KEY)
        ? rawConfig.get(AUTH_SOURCE_CONFIGURATION_KEY).asText(DEFAULT_AUTH_SOURCE)
        : DEFAULT_AUTH_SOURCE;
  }

  public Integer getCheckpointInterval() {
    return rawConfig.has(CHECKPOINT_INTERVAL_CONFIGURATION_KEY)
        ? rawConfig.get(CHECKPOINT_INTERVAL_CONFIGURATION_KEY).asInt(CHECKPOINT_INTERVAL)
        : CHECKPOINT_INTERVAL;
  }

  public String getDatabaseName() {
    return rawConfig.has(DATABASE_CONFIGURATION_KEY) ? rawConfig.get(DATABASE_CONFIGURATION_KEY).asText() : null;
  }

  public OptionalInt getQueueSize() {
    return rawConfig.has(MongoConstants.QUEUE_SIZE_CONFIGURATION_KEY)
        ? OptionalInt.of(rawConfig.get(MongoConstants.QUEUE_SIZE_CONFIGURATION_KEY).asInt())
        : OptionalInt.empty();
  }

  public String getPassword() {
    return rawConfig.has(PASSWORD_CONFIGURATION_KEY) ? rawConfig.get(PASSWORD_CONFIGURATION_KEY).asText() : null;
  }

  public String getUsername() {
    return rawConfig.has(USERNAME_CONFIGURATION_KEY) ? rawConfig.get(USERNAME_CONFIGURATION_KEY).asText() : null;
  }

  public boolean hasAuthCredentials() {
    return rawConfig.has(USERNAME_CONFIGURATION_KEY) && rawConfig.has(PASSWORD_CONFIGURATION_KEY);
  }

  public Integer getSampleSize() {
    if (rawConfig.has(DISCOVER_SAMPLE_SIZE_CONFIGURATION_KEY)) {
      return rawConfig.get(DISCOVER_SAMPLE_SIZE_CONFIGURATION_KEY).asInt(DEFAULT_DISCOVER_SAMPLE_SIZE);
    } else {
      return DEFAULT_DISCOVER_SAMPLE_SIZE;
    }
  }

  public String getCertificate() {
    return rawConfig.has(CERTIFICATE_CONFIGURATION_KEY) ? rawConfig.get(CERTIFICATE_CONFIGURATION_KEY).asText() : null;
  }

  public String getPrivateKey() {
    return rawConfig.has(PRIVATE_KEY_CONFIGURATION_KEY) ? rawConfig.get(PRIVATE_KEY_CONFIGURATION_KEY).asText() : null;
  }

  public String getCertificatePass() {
    return rawConfig.has(CERTIFICATE_PASS_CONFIGURATION_KEY)
        ? rawConfig.get(CERTIFICATE_PASS_CONFIGURATION_KEY).asText()
        : "";
  }
}