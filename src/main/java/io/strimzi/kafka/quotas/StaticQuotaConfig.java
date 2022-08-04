/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.strimzi.kafka.quotas.distributed.KafkaClientManager;
import io.strimzi.kafka.quotas.local.InMemoryQuotaFactorSupplier;
import io.strimzi.kafka.quotas.local.StaticQuotaSupplier;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
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
 * It also serves as a pseudo dependency injection service by supplying configuration details and creating instances.
 * <p>
 * Note the config object has a short lifetime so instances which have a long lifecycle need to be constructed via a factory and managed externally.
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
    static final String VOLUME_USAGE_METRICS_TOPIC_PROP = "client.quota.callback.usageMetrics.topic";
    static final String TOPIC_PARTITION_COUNT_PROP = "client.quota.callback.kafka.partitionCount";
    static final String KAFKA_READ_TIMEOUT_SECONDS_PROP = "client.quota.callback.kafka.readTimeout";
    static final String MISSING_DATA_QUOTA_FACTOR_PROP = "client.quota.callback.missingDataDFactor";
    static final String STALE_METRICS_DATA_PROP = "client.quota.callback.usageMetrics.staleDuration";

    static final String LOG_DIRS_PROP = "log.dirs";
    private static final String SUPER_USERS_CONFIG_PROP = "super.users";

    private final Logger log = LoggerFactory.getLogger(StaticQuotaConfig.class);
    private KafkaClientManager kafkaClientManager;

    /**
     * Construct a configuration for the static quota plugin.
     *
     * @param props the configuration properties
     * @param doLog whether the configurations should be logged
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
                        .define(MISSING_DATA_QUOTA_FACTOR_PROP, DOUBLE, "0.0", ConfigDef.Range.between(0.0, 1.0), MEDIUM, "What factor should be applied if there is insufficient data to make a quota decision. Expressed as a percentage between 0.0 and 1.0")
                        .define(VOLUME_USAGE_METRICS_TOPIC_PROP, STRING, "__strimzi_volumeUsageMetrics", LOW, "topic used to propagate volume usage metrics")
                        .define(TOPIC_PARTITION_COUNT_PROP, INT, "1", LOW, "The number of partitions to use for the topics used by the plugin")
                        .define(KAFKA_READ_TIMEOUT_SECONDS_PROP, INT, "10", LOW, "How long should the plugin wait for kafka interactions")
                        .define(STALE_METRICS_DATA_PROP, STRING, "PT10m", ConfigDef.LambdaValidator.with(StaticQuotaConfig::validateDuration, () -> "DurationValidator"), LOW, "After what java.time.Duration should broker metrics be considered stale and ignored.")
                        .define(LOG_DIRS_PROP, LIST, List.of(), HIGH, "Broker log directories"),
                props,
                doLog);
    }

    /**
     * Injects the kafka client manager which handles construction of the kafka clients and manages their lifecycle.
     *
     * @param kafkaClientManager The client factory instance
     * @return this for use in a fluent builder style.
     */
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
        final Set<String> excludedPrincipals = new HashSet<>(getList(EXCLUDED_PRINCIPAL_NAME_LIST_PROP));
        final Set<String> superPrincipals = getSuperUserPrincipals();
        if (!excludedPrincipals.containsAll(superPrincipals)) {
            excludedPrincipals.addAll(superPrincipals);
        }
        return List.copyOf(excludedPrincipals);
    }

    /**
     * @return The configured source for quotaFactors
     */
    public QuotaFactorSupplier quotaFactorSupplier() {
        return new InMemoryQuotaFactorSupplier();
    }

    /**
     * @return The configured source of VolumeUsageMetrics
     */
    public Supplier<Iterable<VolumeUsageMetrics>> volumeUsageMetricsSupplier() {
        final String volumeUsageMetricsTopic = getString(VOLUME_USAGE_METRICS_TOPIC_PROP);
        final List<TopicPartition> topicPartitions = IntStream.range(0, getPartitionCount()).mapToObj(i -> new TopicPartition(volumeUsageMetricsTopic, i)).collect(Collectors.toList());
        //Note its important that the call to `consumerFor` happens in the supplier instance so that it doesn't
        //get triggered on the thread calling configure and blocking broker start up.
        return () -> {
            List<VolumeUsageMetrics> usageMetrics = new ArrayList<>();
            //TODO nasty doing this in the supplier should potentially be done async.
            //As callers of the supplier don't necessarily expect blocking operations.
            var consumer = kafkaClientManager.consumerFor(VolumeUsageMetrics.class);
            try {
                consumer.assign(topicPartitions);
                consumer.poll(Duration.ofSeconds(getInt(KAFKA_READ_TIMEOUT_SECONDS_PROP)))
                        .records(volumeUsageMetricsTopic)
                        .forEach(cr -> usageMetrics.add(cr.value()));
            } catch (InvalidOffsetException e) {
                log.warn("Invalid offset when polling records. Resetting the offset to latest.", e);
                try {
                    consumer.seekToEnd(topicPartitions);
                } catch (IllegalStateException seekException) {
                    log.error("Resetting offset failed because some partitions are not currently assigned to this consumer.", seekException);
                } catch (IllegalArgumentException seekException) {
                    log.error("Resetting offset failed because the partitions to be reset is null.", seekException);
                }
            } catch (Exception e) {
                log.error("Unable to gather volumeUsageMetrics.", e);
            }
            return usageMetrics;
        };
    }

    /**
     * @return The configured quota source.
     */
    public QuotaSupplier quotaSupplier() {
        return new StaticQuotaSupplier(getQuotaMap());
    }

    /**
     * @return The <a href="https://kafka.apache.org/30/javadoc/org/apache/kafka/common/Node.html#idString%28%29">idString</a> representing the local broker.
     */
    public String getBrokerId() {
        return getBrokerIdFrom(originals());
    }

    public static String getBrokerIdFrom(Map<String, Object> configMap) {
        //Arguably in-efficient to look up the sys prop if we don't need it, but it reads better and is invoked rarely
        final String brokerIdFromSysProps = System.getProperty("broker.id", "-1");
        return (String) configMap.getOrDefault("broker.id", brokerIdFromSysProps);
    }

    public int getReplicationFactor() {
        final String defaultReplicationFactor = (String) originals().getOrDefault("default.replication.factor",  "-1");
        return Integer.parseInt(defaultReplicationFactor);
    }

    /**
     * @return a function which accepts a volume metrics snapshot and sends it to the configured destination
     */
    public Consumer<VolumeUsageMetrics> volumeUsageMetricsPublisher() {
        //TODO connection status metrics
        final String brokerId = getBrokerId();
        final String topic = getString(VOLUME_USAGE_METRICS_TOPIC_PROP);
        //Note its important that the call to `producer` happens in the supplier instance so that it doesn't
        //get triggered on the thread calling configure and blocking broker start up.
        return snapshot -> {
            try {
                kafkaClientManager.producer(VolumeUsageMetrics.class).send(new ProducerRecord<>(topic, brokerId, snapshot), (metadata, exception) -> {
                    if (exception != null) {
                        log.warn("unable to publish message due to: {}", exception.getMessage());
                    }
                });
            } catch (IllegalStateException e) {
                log.warn("Unable to send message: {}", e.getMessage());
            }
        };
    }

    /**
     * @return A function which resolves the set of nodeId's which Kafka considers to be actively part of the cluster
     */
    public Supplier<Collection<String>> activeBrokerSupplier() {
        return () -> activeBrokerNodesSupplier().get().stream().map(Node::idString).collect(Collectors.toSet());
    }

    /**
     * @return A function which resolves the set of nodes which Kafka considers to be actively part of the cluster
     */
    public Supplier<Collection<Node>> activeBrokerNodesSupplier() {
        return () -> {
            try {
                return kafkaClientManager.adminClient().describeCluster().nodes()
                        .get(getInt(KAFKA_READ_TIMEOUT_SECONDS_PROP), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("unable to get current set of broker nodes: {}", e.getMessage(), e);
                return Set.of();
            } catch (ExecutionException | TimeoutException e) {
                log.warn("unable to get current set of broker nodes: {}", e.getMessage(), e);
                return Set.of();
            }
        };
    }

    public double getMissingDataQuotaFactor() {
        return getDouble(MISSING_DATA_QUOTA_FACTOR_PROP);
    }


    public String getVolumeUsageMetricsTopic() {
        return getString(VOLUME_USAGE_METRICS_TOPIC_PROP);
    }

    public int getPartitionCount() {
        return getInt(TOPIC_PARTITION_COUNT_PROP);
    }

    public Duration getMetricsStaleAfterDuration() {
        //We don't need to handle parse errors here as value is validated to be parseable by the config system
        return Duration.parse(getString(STALE_METRICS_DATA_PROP));
    }

    private Set<String> getSuperUserPrincipals() {
        //Can't use getList as SUPER_USERS_CONFIG_PROP is not something from the configDef, and it isn't a comma separated list
        String superUsersConfig = (String) originals().getOrDefault(SUPER_USERS_CONFIG_PROP, "");
        return Arrays.stream(superUsersConfig.split(";"))
                .filter(str -> !str.trim().isEmpty())
                .map(SecurityUtils::parseKafkaPrincipal)
                .map(KafkaPrincipal::getName)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static void validateDuration(String name, Object value) {
        if (value != null && CharSequence.class.isAssignableFrom(value.getClass())) {
            try {
                Duration.parse((CharSequence) value);
                return;
            } catch (Exception e) {
                throw new ConfigException(name, value, e.getMessage());
            }
        }
        throw new ConfigException(name, value, "Invalid duration provided");
    }

}

