/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.distributed;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class KafkaClientManager implements Closeable, Configurable {
    private final ConcurrentMap<Class<?>, Producer<String, ?>> producersByType;

    private final ConcurrentMap<Class<?>, Consumer<String, ?>> consumersByType;

    //using a map for computeIfAbsent semantics.
    private final ConcurrentMap<Class<Admin>, Admin> adminClientHolder;

    private final Function<KafkaClientConfig, KafkaClientFactory> kafkaClientFactorySupplier;

    private final Logger log = getLogger(KafkaClientManager.class);
    private KafkaClientFactory kafkaClientFactory;

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
    public void close() throws IOException {
        for (Producer<String, ?> producer : producersByType.values()) {
            try {
                producer.close();
            } catch (Exception e) {
                log.warn("caught exception closing producer. Continuing to closing others: {}", e.getMessage(), e);
            }
        }
        for (Consumer<String, ?> consumer : consumersByType.values()) {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("caught exception closing consumer. Continuing to closing others: {}", e.getMessage(), e);
            }
        }
        try {
            adminClientHolder.get(Admin.class).close();
        } catch (Exception e) {
            log.warn("caught exception closing consumer. Continuing to closing others: {}", e.getMessage(), e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        kafkaClientFactory = this.kafkaClientFactorySupplier.apply(new KafkaClientConfig(configs, true));
    }

    //TODO @Nullable ?
    @SuppressWarnings("unchecked")
    public <T> Producer<String, T> producer(Class<T> messageType) {
        if (kafkaClientFactory == null) {
            return null;
        }
        return (Producer<String, T>) producersByType.computeIfAbsent(messageType, key -> kafkaClientFactory.newProducer(kafkaClientFactory.getBaseKafkaConfig(), key));
    }

    @SuppressWarnings("unchecked")
    public <T> Consumer<String, T> consumerFor(String topic, Class<T> messageType) {
        if (kafkaClientFactory == null) {
            return null;
        }
        //TODO connection status metrics
        final Consumer<String, T> kafkaConsumer = (Consumer<String, T>) consumersByType.computeIfAbsent(messageType, key -> {
            final Map<String, Object> consumerConfig = kafkaClientFactory.getBaseKafkaConfig();
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, messageType.getSimpleName());
            return kafkaClientFactory.newConsumer(consumerConfig, key);
        });

        //TODO should we really subscribe here?
        kafkaConsumer.subscribe(List.of(topic));
        return kafkaConsumer;
    }

    public Admin adminClient() {
        //TODO connection status metrics
        return adminClientHolder.computeIfAbsent(Admin.class, key -> kafkaClientFactory.newAdmin());
    }
}
