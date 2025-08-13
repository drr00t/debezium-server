/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.falkordb;

import java.util.List;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Field.RangeValidator;
import io.debezium.util.Collect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FalkorDBChangeConsumerConfig  {

    private static final Logger LOGGER = LoggerFactory.getLogger(FalkorDBChangeConsumerConfig.class);
    private static final String PROP_PREFIX = "debezium.sink.";

    public static final String FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING = "falkordb.";

    private static final String DEFAULT_GRAPH_NAME = "g";
    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final Field PROP_BATCH_SIZE = Field.create(FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING + "batch.size")
            .withDefault(DEFAULT_BATCH_SIZE);

    private static final int DEFAULT_MEMORY_LIMIT_MB = 0;
    private static final int DEFAULT_BUFFER_FILL_RATE = 30000;

    private static final Field PROP_GRAPH_NAME = Field.create(FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING + "graph.name")
            .withDefault(DEFAULT_GRAPH_NAME);

    private static final Field PROP_MEMORY_LIMIT_MB = Field.create(FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING + "memory.limit.mb")
            .withDefault(DEFAULT_MEMORY_LIMIT_MB)
            .withValidation(RangeValidator.atLeast(0));
    private static final Field PROP_BUFFER_FILL_RATE = Field.create(FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING + "buffer.fill.rate")
            .withDefault(DEFAULT_BUFFER_FILL_RATE)
            .withValidation(RangeValidator.atLeast(0));

    private static final boolean DEFAULT_SKIP_HEARTBEAT_MESSAGES = true;
    private static final Field PROP_SKIP_HEARTBEAT_MESSAGES = Field.create(FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING + "skip.heartbeat.messages")
            .withDefault(DEFAULT_SKIP_HEARTBEAT_MESSAGES);

    private static final Field PROP_HOST = Field.create(FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING + "host");
    private static final Field PROP_PORT = Field.create(FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING + "port");
    private static final Field PROP_GRAPH_USER = Field.create(FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING + "graph.user");
    private static final Field PROP_GRAPH_USER_PASSWORD = Field.create(FALKORDB_CONFIGURATION_FIELD_PREFIX_STRING + "graph.user.password");

    private int batchSize;
    private int memoryThreshold;
    private int memoryLimitMb;
    private int batchDelay;
    private int bufferFillRate;
    private boolean skipHeartbeatMessages;
    private String host;
    private int port;
    private String graphName;
    private String graphUser;
    private String graphUserPassword;

    public FalkorDBChangeConsumerConfig(Configuration config) {

        config = config.subset(PROP_PREFIX, true);

        LOGGER.info("Configuration for '{}' with prefix '{}': {}", getClass().getSimpleName(), PROP_PREFIX, config.withMaskedPasswords());
        if (!config.validateAndRecord(getAllConfigurationFields(), error -> LOGGER.error("Validation error for property with prefix '{}': {}", PROP_PREFIX, error))) {
            throw new DebeziumException(
                    String.format("Error configuring an instance of '%s' with prefix '%s'; check the logs for errors", getClass().getSimpleName(), PROP_PREFIX));
        }
        init(config);
    }

    protected void init(Configuration config) {
        batchSize = config.getInteger(PROP_BATCH_SIZE);
        memoryLimitMb = config.getInteger(PROP_MEMORY_LIMIT_MB);
        bufferFillRate = config.getInteger(PROP_BUFFER_FILL_RATE);
        skipHeartbeatMessages = config.getBoolean(PROP_SKIP_HEARTBEAT_MESSAGES);
        graphName = config.getString(PROP_GRAPH_NAME);
        graphUser = config.getString(PROP_GRAPH_USER);
        graphUserPassword = config.getString(PROP_GRAPH_USER_PASSWORD);
        host = config.getString(PROP_HOST);
        port = config.getInteger(PROP_PORT);

    }

    protected List<Field> getAllConfigurationFields() {
        List<Field> fields = Collect.arrayListOf(PROP_BATCH_SIZE, PROP_SKIP_HEARTBEAT_MESSAGES);
        return fields;
    }

    public int getBufferFillRate() {
        return bufferFillRate;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBatchDelay() {
        return batchDelay;
    }

    public int getMemoryThreshold() {
        return memoryThreshold;
    }

    public int getMemoryLimitMb() {
        return memoryLimitMb;
    }

    public String getGraphName() { return graphName; }

    public String getGraphUser() { return graphUser; }

    public String getGraphUserPassword() { return graphUserPassword; }

    public String getHost() { return host; }

    public int getPort() { return port; }

    public boolean isSkipHeartbeatMessages() {
        return skipHeartbeatMessages;
    }
}
