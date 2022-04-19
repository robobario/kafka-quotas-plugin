/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.kafka.quotas.json.JacksonDeserializer;
import io.strimzi.kafka.quotas.json.JacksonSerializer;
import io.strimzi.kafka.quotas.local.InMemoryQuotaFactorSupplier;
import io.strimzi.kafka.quotas.local.StaticQuotaSupplier;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
    //TODO DO we really need a name prop
    static final String LISTENER_NAME_PROP = "client.quota.callback.kafka.listener.name";
    static final String LISTENER_PORT_PROP = "client.quota.callback.kafka.listener.port";
    static final String LISTENER_PROTOCOL_PROP = "client.quota.callback.kafka.listener.protocol";
    static final String LOG_DIRS_PROP = "log.dirs";
    private final ObjectMapper objectMapper;

    private final Logger log = LoggerFactory.getLogger(StaticQuotaConfig.class);

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
                        .define(QUOTA_FACTOR_UPDATE_TOPIC_PATTERN_PROP, STRING, "__strimzi_quotaFactorUpdate", LOW, "topic used to update new quota factors to apply to requests")
                        .define(VOLUME_USAGE_METRICS_TOPIC_PROP, STRING, "__strimzi_volumeUsageMetrics", LOW, "topic used to propagate volume usage metrics")
                        .define(LISTENER_NAME_PROP, STRING, "replication-9091", LOW, "which listener to connect to")
                        .define(LISTENER_PORT_PROP, INT, 9091, LOW, "which port to connect to the listener on")
                        .define(LISTENER_PROTOCOL_PROP, STRING, "SSL", LOW, "what protocol to use when connecting to the listener listener")
                        .define(LOG_DIRS_PROP, LIST, List.of(), HIGH, "Broker log directories"),
                props,
                doLog);
        objectMapper = new ObjectMapper();
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
        final String volumeUsageMetricsTopic = getString(VOLUME_USAGE_METRICS_TOPIC_PROP);
        final Map<String, Object> consumerConfig = getKafkaConfig();
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "TODO");
        final KafkaConsumer<String, VolumeUsageMetrics> kafkaConsumer = new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new JacksonDeserializer<>(objectMapper, VolumeUsageMetrics.class));
        //TODO who closes the consumer?
        //TODO should we really subscribe here?
        kafkaConsumer.subscribe(List.of(volumeUsageMetricsTopic));
        return () -> {
            final Iterable<ConsumerRecord<String, VolumeUsageMetrics>> records = kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS)).records(volumeUsageMetricsTopic);
            List<VolumeUsageMetrics> usageMetrics = new ArrayList<>();
            records.forEach(cr -> {
                usageMetrics.add(cr.value());
            });
            return usageMetrics;
        };
    }

    public QuotaSupplier quotaSupplier() {
        return new StaticQuotaSupplier(getQuotaMap());
    }

    public String getBrokerId() {
        //Arguably in-efficient to look up the sys prop if we don't need it, but it reads better and is invoked rarely
        final String brokerIdFromSysProps = System.getProperty("broker.id", "-1");
        return getOriginalConfigString("broker.id", brokerIdFromSysProps);
    }

    public Consumer<VolumeUsageMetrics> volumeUsageMetricsPublisher() {
        final KafkaProducer<String, VolumeUsageMetrics> kafkaProducer = new KafkaProducer<>(getKafkaConfig(), new StringSerializer(), new JacksonSerializer<>(objectMapper));
        final String brokerId = getBrokerId();
        final String topic = getString(VOLUME_USAGE_METRICS_TOPIC_PROP);
        return snapshot -> kafkaProducer.send(new ProducerRecord<>(topic, brokerId, snapshot));
    }

    private Map<String, Object> getKafkaConfig() {
        final String bootstrapAddress = getBootstrapAddress();

        final Map<String, Object> configuredProperties = Stream.concat(Stream.of(
                                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                                        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
                                .map(p -> {
                                    String configKey =
                                            String.format("listener.name.%s.%s", getInt(LISTENER_PORT_PROP), p);
                                    String v = getOriginalConfigString(configKey);
                                    return Map.entry(p, Objects.requireNonNullElse(v, ""));
                                })
                                .filter(e -> !"".equals(e.getValue())),
                        Stream.of(
                                Map.entry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress),
                                Map.entry(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, getString(LISTENER_PROTOCOL_PROP))))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue));
        log.info("resolved kafka config of {}", configuredProperties);
        return configuredProperties;
    }

    private String getOriginalConfigString(String configKey) {
        return (String) originals().get(configKey);
    }

    private String getOriginalConfigString(String configKey, String defaultValue) {
        return (String) originals().getOrDefault(configKey, defaultValue);
    }

    private String getBootstrapAddress() {
        final Integer listenerPort = getInt(LISTENER_PORT_PROP);
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            log.warn("Unable to get canonical hostname for localhost: {} defaulting to 127.0.0.1:{}", e.getMessage(), listenerPort, e);
            hostname = "217.0.0.1";
        }
        return String.format("%s:%s", hostname, listenerPort);
    }
}

