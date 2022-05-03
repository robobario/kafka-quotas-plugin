/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.distributed.KafkaClientManager;
import io.strimzi.kafka.quotas.local.ActiveBrokerQuotaFactorPolicyTask;
import io.strimzi.kafka.quotas.local.UnlimitedQuotaSupplier;
import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows configuring generic quotas for a broker independent of users and clients.
 */
//TODO its not really a static callback anymore
public class StaticQuotaCallback implements ClientQuotaCallback {
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private static final String EXCLUDED_PRINCIPAL_QUOTA_KEY = "excluded-principal-quota-key";
    public static final String METRICS_SCOPE = "io.strimzi.kafka.quotas.StaticQuotaCallback";

    private volatile List<String> excludedPrincipalNameList = List.of();
    private final AtomicBoolean resetQuota = new AtomicBoolean(true);
    private final ScheduledExecutorService executorService;
    private final BiFunction<Map<String, ?>, Boolean, StaticQuotaConfig> pluginConfigFactory;
    private final KafkaClientManager kafkaClientManager;
    private final static long LOGGING_DELAY_MS = 1000;

    //Default to no restrictions until things have been configured.
    private volatile QuotaSupplier quotaSupplier = UnlimitedQuotaSupplier.UNLIMITED_QUOTA_SUPPLIER;
    private volatile QuotaFactorSupplier quotaFactorSupplier = UnlimitedQuotaSupplier.UNLIMITED_QUOTA_SUPPLIER;
    private ScheduledFuture<?> dataSourceFuture;
    private ScheduledFuture<?> quotaPolicyFuture;
    private ScheduledFuture<?> ensureTopicAvailableFuture;

    public StaticQuotaCallback() {
        this(
                Executors.newSingleThreadScheduledExecutor(r -> {
                    final Thread thread = new Thread(r, StaticQuotaCallback.class.getSimpleName() + "-taskExecutor");
                    thread.setDaemon(true);
                    return thread;
                }),
                StaticQuotaConfig::new,
                new KafkaClientManager()
        );
    }

    StaticQuotaCallback(ScheduledExecutorService executorService, BiFunction<Map<String, ?>, Boolean, StaticQuotaConfig> pluginConfigFactory, KafkaClientManager kafkaClientManager) {
        this.executorService = executorService;
        this.pluginConfigFactory = pluginConfigFactory;
        this.kafkaClientManager = kafkaClientManager;
    }

    public static MetricName metricName(Class<?> clazz, String name) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        return metricName(group, name, type);
    }

    public static MetricName metricName(String group, String name, String type) {
//        private MetricName metricName(Class<?> clazz, String name) {
//            String group = clazz.getPackageName();
//            String type = clazz.getSimpleName();
//            String mBeanName = String.format("%s:type=%s,name=%s", group, type, name);
//            return new MetricName(group, type, name, this.scope, mBeanName);
//        }
        String mBeanName = String.format("%s:type=%s,name=%s", group, type, name);
        return new MetricName(group, type, name, METRICS_SCOPE, mBeanName);
    }

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        Map<String, String> m = new HashMap<>();
        m.put("quota.type", quotaType.name());
        if (!excludedPrincipalNameList.isEmpty() && principal != null && excludedPrincipalNameList.contains(principal.getName())) {
            m.put(EXCLUDED_PRINCIPAL_QUOTA_KEY, Boolean.TRUE.toString());
        }
        return m;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        if (Boolean.TRUE.toString().equals(metricTags.get(EXCLUDED_PRINCIPAL_QUOTA_KEY))) {
            return QuotaSupplier.UNLIMITED;
        }

        final double requestQuota = quotaSupplier.quotaFor(quotaType, metricTags);
        if (ClientQuotaType.PRODUCE.equals(quotaType)) {
            //Kafka will suffer an A divide by zero if returned 0.0 from `quotaLimit` so ensure that we don't even if we have zero quota available
            return Math.max(requestQuota * quotaFactorSupplier.get(), QuotaSupplier.PAUSED);
        }
        return requestQuota;
    }

    /**
     * Put a small delay between logging
     */
    private void maybeLog(AtomicLong lastLoggedMessageTimeMs, String format, Object... args) {
        if (log.isDebugEnabled()) {
            long now = System.currentTimeMillis();
            final boolean[] shouldLog = {true};
            lastLoggedMessageTimeMs.getAndUpdate(current -> {
                if (now - current >= LOGGING_DELAY_MS) {
                    shouldLog[0] = true;
                    return now;
                }
                shouldLog[0] = false;
                return current;
            });
            if (shouldLog[0]) {
                log.debug(format, args);
            }
        }
    }

    @Override
    public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public boolean quotaResetRequired(ClientQuotaType quotaType) {
        return resetQuota.getAndSet(false);
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        return false;
    }

    @Override
    public void close() {
        try {
            closeExecutorService();
            closeKafkaClients();
        } finally {
            Metrics.defaultRegistry().allMetrics().keySet().stream()
                    .filter(m -> METRICS_SCOPE.equals(m.getScope()))
                    .forEach(Metrics.defaultRegistry()::removeMetric);
        }
    }

    private void closeExecutorService() {
        try {
            executorService.shutdownNow();
        } catch (Exception e) {
            log.warn("Encountered problem shutting down background executor: {}", e.getMessage(), e);
        }
    }

    private void closeKafkaClients() {
        try {
            kafkaClientManager.close();
        } catch (Exception e) {
            log.warn("Encountered problem shutting down Kafka Clients: {}", e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        kafkaClientManager.configure(configs);
        StaticQuotaConfig config = pluginConfigFactory.apply(configs, true).withKafkaClientManager(kafkaClientManager);
        quotaSupplier = config.quotaSupplier();
        quotaFactorSupplier = config.quotaFactorSupplier();
        excludedPrincipalNameList = config.getExcludedPrincipalNameList();

        quotaFactorSupplier.addUpdateListener(() -> resetQuota.set(true));

        List<Path> logDirs = config.getLogDirs().stream().map(Paths::get).collect(Collectors.toList());

        if (config.getStorageCheckInterval() > 0) {
            ensureExistingTaskCancelled(ensureTopicAvailableFuture, dataSourceFuture);
            String topic = config.getVolumeUsageMetricsTopic();
            ensureTopicAvailableFuture = executorService.scheduleWithFixedDelay(new EnsureTopicAvailableRunnable(kafkaClientManager, topic, config.getPartitionCount()), 0, config.getStorageCheckInterval(), TimeUnit.SECONDS);

            final FileSystemDataSourceTask fileSystemDataSourceTask = new FileSystemDataSourceTask(logDirs, config.getSoftLimit(), config.getHardLimit(), config.getStorageCheckInterval(), config.getBrokerId(), config.volumeUsageMetricsPublisher());
            dataSourceFuture = executorService.scheduleWithFixedDelay(fileSystemDataSourceTask, 0, fileSystemDataSourceTask.getPeriod(), fileSystemDataSourceTask.getPeriodUnit());
        }

        if (config.getQuotaPolicyInterval() > 0) {
            ensureExistingTaskCancelled(quotaPolicyFuture);
            final QuotaFactorPolicyTask quotaFactorPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(config.getQuotaPolicyInterval(), config.volumeUsageMetricsSupplier(), config.activeBrokerSupplier(), config.getMissingDataQuotaFactor(), config.getMetricsStaleAfterDuration());
            if (quotaFactorSupplier.getClass().isAssignableFrom(Consumer.class)) {
                quotaFactorPolicyTask.addListener((Consumer<UpdateQuotaFactor>) quotaFactorSupplier);
            }
            quotaPolicyFuture = executorService.scheduleWithFixedDelay(quotaFactorPolicyTask, 0, quotaFactorPolicyTask.getPeriod(), quotaFactorPolicyTask.getPeriodUnit());
        }
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }
    }

    private void ensureExistingTaskCancelled(ScheduledFuture<?>... scheduledFutures) {
        for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
        }
    }
}
