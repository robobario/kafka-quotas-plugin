/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.json;

import java.io.IOException;
import java.lang.reflect.Type;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class JacksonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper;
    private final TypeReference<T> objectType;

    private final Logger log = getLogger(JacksonDeserializer.class);

    public JacksonDeserializer(ObjectMapper objectMapper, Class<T> objectType) {
        this(objectMapper, new TypeReference<>() {
            @Override
            public Type getType() {
                return objectType;
            }
        });
    }

    public JacksonDeserializer(ObjectMapper objectMapper, TypeReference<T> objectType) {
        this.objectMapper = objectMapper;
        this.objectType = objectType;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, objectType);
        } catch (IOException e) {
            log.error("Something very strange happened, we got an IOException ({}) reading from a byte array", e.getMessage(), e);
            return null;
        }
    }
}
