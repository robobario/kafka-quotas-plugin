/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.distributed;

import java.io.Closeable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import io.strimzi.kafka.quotas.StaticQuotaConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;

import static io.strimzi.kafka.quotas.distributed.KafkaClientFactory.CLIENT_ID_PREFIX_PROP;
import static org.slf4j.LoggerFactory.getLogger;

public class KafkaClientManager implements Closeable, Configurable {
    private static final IllegalStateException NO_CLIENT_MANAGER_EXCEPTION = new IllegalStateException("no kafkaClientFactory available. Ensure it is provided before constructing clients");
    public static final Duration CLOSE_TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);
    private final ConcurrentMap<Class<?>, Producer<String, ?>> producersByType;

    private final ConcurrentMap<Class<?>, Consumer<String, ?>> consumersByType;

    //using a map for computeIfAbsent semantics.
    private final ConcurrentMap<Class<Admin>, Admin> adminClientHolder;

    private final Function<KafkaClientConfig, KafkaClientFactory> kafkaClientFactorySupplier;

    private final Logger log = getLogger(KafkaClientManager.class);
    private KafkaClientFactory kafkaClientFactory;
    private String brokerId;
    private volatile KafkaClientConfig kafkaClientConfig;

    public KafkaClientManager() {
        this(KafkaClientFactory::new);
    }

    KafkaClientManager(Function<KafkaClientConfig, KafkaClientFactory> kafkaClientFactorySupplier) {
        producersByType = new ConcurrentHashMap<>();
        consumersByType = new ConcurrentHashMap<>();
        adminClientHolder = new ConcurrentHashMap<>(1);
        this.kafkaClientFactorySupplier = kafkaClientFactorySupplier;
    }

    @Override
    public void close() {
        for (Producer<String, ?> producer : producersByType.values()) {
            try {
                producer.close(CLOSE_TIMEOUT);
            } catch (Exception e) {
                log.warn("caught exception closing producer. Continuing to closing others: {}", e.getMessage(), e);
            }
        }
        for (Consumer<String, ?> consumer : consumersByType.values()) {
            try {
                consumer.close(CLOSE_TIMEOUT);
            } catch (Exception e) {
                log.warn("caught exception closing consumer. Continuing to closing others: {}", e.getMessage(), e);
            }
        }
        try {
            adminClientHolder.get(Admin.class).close(CLOSE_TIMEOUT);
        } catch (Exception e) {
            log.warn("caught exception closing adminClient: {}", e.getMessage(), e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        kafkaClientConfig = new KafkaClientConfig(configs, true);
        kafkaClientFactory = this.kafkaClientFactorySupplier.apply(kafkaClientConfig);
        brokerId = StaticQuotaConfig.getBrokerIdFrom(kafkaClientConfig.originals());
    }

    @SuppressWarnings("unchecked")
    public <T> Producer<String, T> producer(Class<T> messageType) {
        if (kafkaClientFactory == null || kafkaClientConfig == null) {
            throw NO_CLIENT_MANAGER_EXCEPTION;
        }
        return (Producer<String, T>) producersByType.computeIfAbsent(messageType, key -> {
            //Disable batching as we have small and comparatively rarely published messages.
            //Reduced acks to 1 as we want to keep publishing regardless of cluster health (as this is one metric of cluster health)
            //Acks=1 implicitly disables idempotence but make it explicit here, so we know its deliberate and acknowledged.
            final Map<String, Object> customConfig = Map.of(
                    ProducerConfig.BATCH_SIZE_CONFIG, 0,
                    ProducerConfig.CLIENT_ID_CONFIG, buildClientId("producer", messageType),
                    ProducerConfig.ACKS_CONFIG, "1",
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false
            );
            return kafkaClientFactory.newProducer(customConfig, key);
        });
    }

    @SuppressWarnings("unchecked")
    public <T> Consumer<String, T> consumerFor(String topic, Class<T> messageType) {
        if (kafkaClientFactory == null || kafkaClientConfig == null) {
            throw NO_CLIENT_MANAGER_EXCEPTION;
        }
        //TODO connection status metrics
        final Consumer<String, T> kafkaConsumer = (Consumer<String, T>) consumersByType.computeIfAbsent(messageType, key -> {
            final Map<String, Object> customConfig = Map.of(
                    ConsumerConfig.CLIENT_ID_CONFIG, buildClientId("consumer", messageType),
                    ConsumerConfig.GROUP_ID_CONFIG, messageType.getSimpleName() + "-" + brokerId,
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            return kafkaClientFactory.newConsumer(customConfig, key);
        });

        kafkaConsumer.subscribe(List.of(topic));
        return kafkaConsumer;
    }

    public Admin adminClient() {
        //TODO connection status metrics
        return adminClientHolder.computeIfAbsent(Admin.class, key -> kafkaClientFactory.newAdmin(Map.of(AdminClientConfig.CLIENT_ID_CONFIG, buildAdminClientId())));
    }

    private <T> String buildClientId(String clientType, Class<T> messageType) {
        final String idPrefix = kafkaClientConfig.getString(CLIENT_ID_PREFIX_PROP);
        return String.format("%s-%s-%s-%s", idPrefix, clientType, messageType.getSimpleName(), brokerId);
    }

    private <T> String buildAdminClientId() {
        final String idPrefix = kafkaClientConfig.getString(CLIENT_ID_PREFIX_PROP);
        return String.format("%s-adminClient-%s", idPrefix, brokerId);
    }
}
