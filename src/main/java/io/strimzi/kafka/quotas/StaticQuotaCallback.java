/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
import io.strimzi.kafka.quotas.local.ActiveBrokerQuotaPolicyTask;
import io.strimzi.kafka.quotas.local.UnlimitedQuotaSupplier;
import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.TopicExistsException;
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

    private final AtomicLong storageUsed = new AtomicLong(0);
    private volatile long storageQuotaSoft = Long.MAX_VALUE;
    private volatile long storageQuotaHard = Long.MAX_VALUE;
    private volatile List<String> excludedPrincipalNameList = List.of();
    private final AtomicBoolean resetQuota = new AtomicBoolean(true);
    private final StorageChecker storageChecker;
    private final ScheduledExecutorService executorService;
    private final BiFunction<Map<String, ?>, Boolean, StaticQuotaConfig> pluginConfigFactory;
    private final KafkaClientManager kafkaClientManager;
    private final static long LOGGING_DELAY_MS = 1000;
    private AtomicLong lastLoggedMessageSoftTimeMs = new AtomicLong(0);
    private AtomicLong lastLoggedMessageHardTimeMs = new AtomicLong(0);

    //Default to no restrictions until things have been configured.
    private volatile QuotaSupplier quotaSupplier = UnlimitedQuotaSupplier.UNLIMITED_QUOTA_SUPPLIER;
    private volatile QuotaFactorSupplier quotaFactorSupplier = UnlimitedQuotaSupplier.UNLIMITED_QUOTA_SUPPLIER;
    private ScheduledFuture<?> dataSourceFuture;
    private ScheduledFuture<?> quotaPolicyFuture;

    public StaticQuotaCallback() {
        this(new StorageChecker(),
                Executors.newSingleThreadScheduledExecutor(r -> {
                    final Thread thread = new Thread(r, StaticQuotaCallback.class.getSimpleName() + "-taskExecutor");
                    thread.setDaemon(true);
                    return thread;
                }),
                StaticQuotaConfig::new,
                new KafkaClientManager()
        );
    }

    StaticQuotaCallback(StorageChecker storageChecker, ScheduledExecutorService executorService, BiFunction<Map<String, ?>, Boolean, StaticQuotaConfig> pluginConfigFactory, KafkaClientManager kafkaClientManager) {
        this.storageChecker = storageChecker;
        this.executorService = executorService;
        this.pluginConfigFactory = pluginConfigFactory;
        this.kafkaClientManager = kafkaClientManager;
    }

    public static MetricName metricName(Class<?> clazz, String name) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        String mBeanName = String.format("%s:type=%s,name=%s", group, type, name);
        return new MetricName(group, type, name, "io.strimzi.kafka.quotas.StaticQuotaCallback", mBeanName);
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
            return  Math.max(requestQuota * quotaFactorSupplier.get(), 1.0);
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
        storageChecker.startIfNecessary();
        return false;
    }

    @Override
    public void close() {
        try {
            executorService.shutdownNow();
            kafkaClientManager.close();
            storageChecker.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> "io.strimzi.kafka.quotas.StaticQuotaCallback".equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        kafkaClientManager.configure(configs);
        StaticQuotaConfig config = pluginConfigFactory.apply(configs, true).withKafkaClientManager(kafkaClientManager);
        quotaSupplier = config.quotaSupplier();
        quotaFactorSupplier = config.quotaFactorSupplier();
        storageQuotaSoft = config.getSoftStorageQuota();
        storageQuotaHard = config.getHardStorageQuota();
        excludedPrincipalNameList = config.getExcludedPrincipalNameList();

        quotaFactorSupplier.addUpdateListener(() -> resetQuota.set(true));

        long storageCheckIntervalMillis = TimeUnit.SECONDS.toMillis(config.getStorageCheckInterval());
        List<Path> logDirs = config.getLogDirs().stream().map(Paths::get).collect(Collectors.toList());
        storageChecker.configure(storageCheckIntervalMillis,
                logDirs,
                this::updateUsedStorage);

        if (dataSourceFuture != null) {
            dataSourceFuture.cancel(false);
        }
        if (config.getStorageCheckInterval() > 0) {
            final FileSystemDataSourceTask fileSystemDataSourceTask = new FileSystemDataSourceTask(logDirs, config.getSoftLimit(), config.getHardLimit(), config.getStorageCheckInterval(), config.getBrokerId(), config.volumeUsageMetricsPublisher());
            dataSourceFuture = executorService.scheduleWithFixedDelay(fileSystemDataSourceTask, 0, fileSystemDataSourceTask.getPeriod(), fileSystemDataSourceTask.getPeriodUnit());
        }

        if (quotaPolicyFuture != null) {
            quotaPolicyFuture.cancel(false);
        }
        //TODO add separate poll interval for quota policy
        if (config.getQuotaPolicyInterval() > 0) {
            final QuotaPolicyTask quotaPolicyTask = new ActiveBrokerQuotaPolicyTask(config.getQuotaPolicyInterval(), config.volumeUsageMetricsSupplier(), config.activeBrokerSupplier());
            if (quotaFactorSupplier.getClass().isAssignableFrom(Consumer.class)) {
                quotaPolicyTask.addListener((Consumer<UpdateQuotaFactor>) quotaFactorSupplier);
            }
            String topic = config.getVolumeUsageMetricsTopic();
            executorService.schedule(() -> {
                try {
                    ensureTopicIsAvailable(topic, config).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("problem ensuring topic {} is available on the cluster due to: {}", topic, e);
                } catch (ExecutionException e) {
                    log.error("problem ensuring topic {} is available on the cluster due to: {}", topic, e);
                }
            }, 0, TimeUnit.SECONDS);
            quotaPolicyFuture = executorService.scheduleWithFixedDelay(quotaPolicyTask, 0, quotaPolicyTask.getPeriod(), quotaPolicyTask.getPeriodUnit());
        }
        //TODO This doesn't really make sense to log here any more, but is useful to have
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }
    }

    /*test*/ CompletableFuture<Void> ensureTopicIsAvailable(String topic, StaticQuotaConfig config) {
        final CompletableFuture<Void> createTopicFuture = new CompletableFuture<>();
        log.info("ensuring {} exists", topic);
        final int topicPartitionCount = config.getPartitionCount();
        final List<NewTopic> topics = List.of(new NewTopic(topic, Optional.of(topicPartitionCount), Optional.empty()));
        kafkaClientManager.adminClient()
                .createTopics(topics)
                .all()
                .whenComplete((unused, throwable) -> {
                    if (throwable != null && !(throwable instanceof TopicExistsException || throwable.getCause() instanceof TopicExistsException)) {
                        log.warn("Error creating topic: {}: {}", topic, throwable.getMessage(), throwable);
                        createTopicFuture.completeExceptionally(throwable);
                        return;
                    }
                    log.info("Created topic: {}", topic);
                    createTopicFuture.complete(null);
                });
        //TODO should we return this or just wait here?
        return createTopicFuture;
    }

    private void updateUsedStorage(Long newValue) {
        var oldValue = storageUsed.getAndSet(newValue);
        if (oldValue != newValue) {
            resetQuota.set(true);
        }
    }
}
