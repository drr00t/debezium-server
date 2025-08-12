/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.falkordb;

import java.util.List;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.config.Field.RangeValidator;
import io.debezium.storage.redis.RedisCommonConfig;
import io.debezium.util.Collect;

public class FalkorDBChangeConsumerConfig extends RedisCommonConfig {

    private static final String PROP_PREFIX = "debezium.sink.";

    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final Field PROP_BATCH_SIZE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "batch.size")
            .withDefault(DEFAULT_BATCH_SIZE);

    private static final int DEFAULT_MEMORY_LIMIT_MB = 0;
    private static final int DEFAULT_BUFFER_FILL_RATE = 30000;
    private static final Field PROP_MEMORY_LIMIT_MB = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "memory.limit.mb")
            .withDefault(DEFAULT_MEMORY_LIMIT_MB)
            .withValidation(RangeValidator.atLeast(0));
    private static final Field PROP_BUFFER_FILL_RATE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "buffer.fill.rate")
            .withDefault(DEFAULT_BUFFER_FILL_RATE)
            .withValidation(RangeValidator.atLeast(0));

    private static final boolean DEFAULT_SKIP_HEARTBEAT_MESSAGES = true;
    private static final Field PROP_SKIP_HEARTBEAT_MESSAGES = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "skip.heartbeat.messages")
            .withDefault(DEFAULT_SKIP_HEARTBEAT_MESSAGES);

    private int batchSize;
    private int memoryThreshold;
    private int memoryLimitMb;
    private int batchDelay;
    private int bufferFillRate;
    private boolean skipHeartbeatMessages;

    public FalkorDBChangeConsumerConfig(Configuration config) {
        super(config, PROP_PREFIX);
    }

    @Override
    protected void init(Configuration config) {
        super.init(config);
        batchSize = config.getInteger(PROP_BATCH_SIZE);
        memoryLimitMb = config.getInteger(PROP_MEMORY_LIMIT_MB);
        bufferFillRate = config.getInteger(PROP_BUFFER_FILL_RATE);
        skipHeartbeatMessages = config.getBoolean(PROP_SKIP_HEARTBEAT_MESSAGES);
    }

    @Override
    protected List<Field> getAllConfigurationFields() {
        List<Field> fields = Collect.arrayListOf(PROP_BATCH_SIZE, PROP_SKIP_HEARTBEAT_MESSAGES);
        fields.addAll(super.getAllConfigurationFields());
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

    public boolean isSkipHeartbeatMessages() {
        return skipHeartbeatMessages;
    }
}
