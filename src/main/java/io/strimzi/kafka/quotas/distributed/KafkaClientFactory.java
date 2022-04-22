/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.distributed;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.strimzi.kafka.quotas.json.JacksonDeserializer;
import io.strimzi.kafka.quotas.json.JacksonSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class KafkaClientFactory {
    private final Logger log = getLogger(KafkaClientFactory.class);

    private final KafkaClientConfig kafkaClientConfig;
    private final StringSerializer keySerializer;
    private final ObjectMapper objectMapper;
    private final StringDeserializer keyDeserializer;

    static final String LISTENER_NAME_PROP = "client.quota.callback.kafka.listener.name";
    static final String LISTENER_PORT_PROP = "client.quota.callback.kafka.listener.port";
    static final String LISTENER_PROTOCOL_PROP = "client.quota.callback.kafka.listener.protocol";

    public KafkaClientFactory(KafkaClientConfig kafkaClientConfig) {
        this.kafkaClientConfig = kafkaClientConfig;
        keySerializer = new StringSerializer();
        keyDeserializer = new StringDeserializer();
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    public <V> Producer<String, V> newProducer(Map<String, Object> kafkaConfig, Class<V> ignored) {
        //Include the value class in the method signature to make mocking easier...
        return new KafkaProducer<>(kafkaConfig, keySerializer, new JacksonSerializer<>(objectMapper));
    }

    public <V> Consumer<String, V> newConsumer(Map<String, Object> kafkaConfig, Class<V> messageType) {
        return new KafkaConsumer<>(kafkaConfig, keyDeserializer, new JacksonDeserializer<>(objectMapper, messageType));
    }

    public Admin newAdmin() {
        return Admin.create(getBaseKafkaConfig());
    }

    public Map<String, Object> getBaseKafkaConfig() {
        final String bootstrapAddress = getBootstrapAddress();

        final Map<String, Object> configuredProperties = Stream.concat(
                        Stream.of(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                                        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
                                .map(p -> {
                                    String configKey = String.format("listener.name.%s.%s", kafkaClientConfig.getString(LISTENER_NAME_PROP), p);
                                    String v = getOriginalConfigString(configKey);
                                    return Map.entry(p, Objects.requireNonNullElse(v, ""));
                                }).filter(e -> !"".equals(e.getValue())),
                        Stream.of(Map.entry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress),
                                Map.entry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaClientConfig.getString(LISTENER_PROTOCOL_PROP))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        log.info("resolved kafka config of {}", configuredProperties);
        return configuredProperties;
    }

    private String getOriginalConfigString(String configKey) {
        return (String) kafkaClientConfig.originals().get(configKey);
    }

    private String getBootstrapAddress() {
        final Integer listenerPort = kafkaClientConfig.getInt(LISTENER_PORT_PROP);
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            log.warn("Unable to get canonical hostname for localhost: {} defaulting to 127.0.0.1:{}", e.getMessage(), listenerPort, e);
            hostname = "127.0.0.1";
        }
        return String.format("%s:%s", hostname, listenerPort);
    }
}
