/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.strimzi.kafka.quotas.distributed.KafkaClientManager;
import io.strimzi.kafka.quotas.local.InMemoryQuotaFactorSupplier;
import io.strimzi.kafka.quotas.local.StaticQuotaSupplier;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

/**
 * Configuration for the static quota plugin.
 */
public class StaticQuotaConfig extends AbstractConfig {
    static final String PRODUCE_QUOTA_PROP = "client.quota.callback.static.produce";
    static final String FETCH_QUOTA_PROP = "client.quota.callback.static.fetch";
    static final String REQUEST_QUOTA_PROP = "client.quota.callback.static.request";
    static final String EXCLUDED_PRINCIPAL_NAME_LIST_PROP = "client.quota.callback.static.excluded.principal.name.list";
    static final String STORAGE_QUOTA_SOFT_PROP = "client.quota.callback.static.storage.soft";
    static final String STORAGE_QUOTA_HARD_PROP = "client.quota.callback.static.storage.hard";
    static final String STORAGE_CHECK_INTERVAL_PROP = "client.quota.callback.static.storage.check-interval";
    static final String QUOTA_POLICY_INTERVAL_PROP = "client.quota.callback.quotaPolicy.check-interval";
    static final String QUOTA_FACTOR_UPDATE_TOPIC_PATTERN_PROP = "client.quota.callback.quotaFactor.topicPattern";
    static final String VOLUME_USAGE_METRICS_TOPIC_PROP = "client.quota.callback.usageMetrics.topic";

    static final String LOG_DIRS_PROP = "log.dirs";

    private final Logger log = LoggerFactory.getLogger(StaticQuotaConfig.class);
    private KafkaClientManager kafkaClientManager;

    /**
     * Construct a configuration for the static quota plugin.
     *
     * @param props              the configuration properties
     * @param doLog              whether the configurations should be logged
     */
    public StaticQuotaConfig(Map<String, ?> props, boolean doLog) {
        super(new ConfigDef()
                        .define(PRODUCE_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Produce bandwidth rate quota (in bytes)")
                        .define(FETCH_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Consume bandwidth rate quota (in bytes)")
                        .define(REQUEST_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Request processing time quota (in seconds)")
                        .define(EXCLUDED_PRINCIPAL_NAME_LIST_PROP, LIST, List.of(), MEDIUM, "List of principals that are excluded from the quota")
                        .define(STORAGE_QUOTA_SOFT_PROP, LONG, Long.MAX_VALUE, HIGH, "Hard limit for amount of storage allowed (in bytes)")
                        .define(STORAGE_QUOTA_HARD_PROP, LONG, Long.MAX_VALUE, HIGH, "Soft limit for amount of storage allowed (in bytes)")
                        .define(STORAGE_CHECK_INTERVAL_PROP, INT, 0, MEDIUM, "Interval between storage check runs (in seconds, default of 0 means disabled")
                        .define(QUOTA_POLICY_INTERVAL_PROP, INT, 0, MEDIUM, "Interval between quota policy runs (in seconds, default of 0 means disabled")
                        .define(QUOTA_FACTOR_UPDATE_TOPIC_PATTERN_PROP, STRING, "__strimzi_quotaFactorUpdate", LOW, "topic used to update new quota factors to apply to requests")
                        .define(VOLUME_USAGE_METRICS_TOPIC_PROP, STRING, "__strimzi_volumeUsageMetrics", LOW, "topic used to propagate volume usage metrics")
                        .define(LOG_DIRS_PROP, LIST, List.of(), HIGH, "Broker log directories"),
                props,
                doLog);
    }

    public StaticQuotaConfig withKafkaClientManager(KafkaClientManager kafkaClientManager) {
        this.kafkaClientManager = kafkaClientManager;
        return this;
    }

    Map<ClientQuotaType, Quota> getQuotaMap() {
        Map<ClientQuotaType, Quota> m = new HashMap<>();
        Double produceBound = getDouble(PRODUCE_QUOTA_PROP);
        Double fetchBound = getDouble(FETCH_QUOTA_PROP);
        Double requestBound = getDouble(REQUEST_QUOTA_PROP);

        m.put(ClientQuotaType.PRODUCE, Quota.upperBound(produceBound));
        m.put(ClientQuotaType.FETCH, Quota.upperBound(fetchBound));
        m.put(ClientQuotaType.REQUEST, Quota.upperBound(requestBound));

        return m;
    }

    long getHardStorageQuota() {
        return getLong(STORAGE_QUOTA_HARD_PROP);
    }

    long getSoftStorageQuota() {
        return getLong(STORAGE_QUOTA_SOFT_PROP);
    }

    Limit getHardLimit() {
        return new Limit(Limit.LimitType.CONSUMED_BYTES, getLong(STORAGE_QUOTA_HARD_PROP));
    }

    Limit getSoftLimit() {
        return new Limit(Limit.LimitType.CONSUMED_BYTES, getLong(STORAGE_QUOTA_SOFT_PROP));
    }

    int getStorageCheckInterval() {
        return getInt(STORAGE_CHECK_INTERVAL_PROP);
    }

    int getQuotaPolicyInterval() {
        return getInt(QUOTA_POLICY_INTERVAL_PROP);
    }

    List<String> getLogDirs() {
        return getList(LOG_DIRS_PROP);
    }

    List<String> getExcludedPrincipalNameList() {
        return getList(EXCLUDED_PRINCIPAL_NAME_LIST_PROP);
    }

    public QuotaFactorSupplier quotaFactorSupplier() {
        return new InMemoryQuotaFactorSupplier();
//        final String factorUpdateTopicPattern = getString(QUOTA_FACTOR_UPDATE_TOPIC_PATTERN);
//        final KafkaConsumer<String, UpdateQuotaFactor> kafkaConsumer = new KafkaConsumer<>(getKafkaConfig(), new StringDeserializer(), new JacksonDeserializer<>(objectMapper, UpdateQuotaFactor.class));
//        //TODO who closes the consumer?
//        final KafkaQuotaFactorSupplier kafkaQuotaFactorSupplier = new KafkaQuotaFactorSupplier(factorUpdateTopicPattern, kafkaConsumer);
//        //TODO should we really start here?
//        kafkaQuotaFactorSupplier.start();
//        return kafkaQuotaFactorSupplier;
    }

    public Supplier<Iterable<VolumeUsageMetrics>> volumeUsageMetricsSupplier() {
        return rawKafkaConsumer();
    }

    private Supplier<Iterable<VolumeUsageMetrics>> rawKafkaConsumer() {
        final String volumeUsageMetricsTopic = getString(VOLUME_USAGE_METRICS_TOPIC_PROP);
        final  org.apache.kafka.clients.consumer.Consumer<String, VolumeUsageMetrics> kafkaConsumer = kafkaClientManager.consumerFor(volumeUsageMetricsTopic, VolumeUsageMetrics.class);
        return () -> {
            List<VolumeUsageMetrics> usageMetrics = new ArrayList<>();
            //TODO nasty doing this in the supplier should really be done async
            kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS))
                    .records(volumeUsageMetricsTopic)
                    .forEach(cr -> usageMetrics.add(cr.value()));
            return usageMetrics;
        };
    }

    public QuotaSupplier quotaSupplier() {
        return new StaticQuotaSupplier(getQuotaMap());
    }

    public String getBrokerId() {
        //Arguably in-efficient to look up the sys prop if we don't need it, but it reads better and is invoked rarely
        final String brokerIdFromSysProps = System.getProperty("broker.id", "-1");
        return (String) originals().getOrDefault("broker.id", brokerIdFromSysProps);
    }

    public Consumer<VolumeUsageMetrics> volumeUsageMetricsPublisher() {
        //TODO lifecycle management
        //TODO connection status metrics
        Producer<String, VolumeUsageMetrics> kafkaProducer = kafkaClientManager.producer(VolumeUsageMetrics.class);
        final String brokerId = getBrokerId();
        final String topic = getString(VOLUME_USAGE_METRICS_TOPIC_PROP);
        return snapshot -> kafkaProducer.send(new ProducerRecord<>(topic, brokerId, snapshot));
    }

    public Supplier<Collection<String>> activeBrokerSupplier() {
        final Admin admin = kafkaClientManager.adminClient();
        return () -> {
            try {
                return admin.describeCluster().nodes().thenApply(nodes -> nodes.stream().map(Node::idString).collect(Collectors.toSet())).get(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("unable to get current set of active brokers: {}", e.getMessage(), e);
                return Set.of();
            } catch (ExecutionException | TimeoutException e) {
                log.warn("unable to get current set of active brokers: {}", e.getMessage(), e);
                return Set.of();
            }
        };
    }
}

