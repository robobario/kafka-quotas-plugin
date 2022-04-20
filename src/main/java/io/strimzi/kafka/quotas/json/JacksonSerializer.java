/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class JacksonSerializer<T> implements Serializer<T> {
    public static final byte[] EMPTY_BYTE_ARRAY = {};
    private final ObjectMapper objectMapper;
    private final Logger log = getLogger(JacksonSerializer.class);

    public JacksonSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.warn("Error translating {} to json: {}", data, e.getMessage(), e);
            return EMPTY_BYTE_ARRAY;
        }
    }
}
