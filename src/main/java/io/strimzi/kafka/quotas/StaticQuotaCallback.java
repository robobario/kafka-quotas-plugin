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
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.local.QuotaPolicyTaskImpl;
import io.strimzi.kafka.quotas.local.UnlimitedQuotaSupplier;
import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.Quota;
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
    private final static long LOGGING_DELAY_MS = 1000;
    private AtomicLong lastLoggedMessageSoftTimeMs = new AtomicLong(0);
    private AtomicLong lastLoggedMessageHardTimeMs = new AtomicLong(0);
    private final String scope = "io.strimzi.kafka.quotas.StaticQuotaCallback";

    //Default to no restrictions until things have been configured.
    private volatile QuotaSupplier staticQuotaSupplier = UnlimitedQuotaSupplier.UNLIMITED_QUOTA_SUPPLIER;
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
                StaticQuotaConfig::new
        );
    }

    StaticQuotaCallback(StorageChecker storageChecker, ScheduledExecutorService executorService, BiFunction<Map<String, ?>, Boolean, StaticQuotaConfig> pluginConfigFactory) {
        this.storageChecker = storageChecker;
        this.executorService = executorService;
        this.pluginConfigFactory = pluginConfigFactory;
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

        final double requestQuota = staticQuotaSupplier.quotaFor(quotaType, metricTags);
        if (ClientQuotaType.PRODUCE.equals(quotaType)) {
            return requestQuota * quotaFactorSupplier.get();
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
            storageChecker.stop();
            executorService.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> scope.equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        StaticQuotaConfig config = pluginConfigFactory.apply(configs, true);
        staticQuotaSupplier = config.quotaSupplier();
        quotaFactorSupplier = config.quotaFactorSupplier();
        storageQuotaSoft = config.getSoftStorageQuota();
        storageQuotaHard = config.getHardStorageQuota();
        excludedPrincipalNameList = config.getExcludedPrincipalNameList();

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
            final QuotaPolicyTask quotaPolicyTask = new QuotaPolicyTaskImpl(config.getQuotaPolicyInterval(), config.volumeUsageMetricsSupplier());
            if (quotaFactorSupplier.getClass().isAssignableFrom(Consumer.class)) {
                quotaPolicyTask.addListener((Consumer<UpdateQuotaFactor>) quotaFactorSupplier);
            }
            quotaPolicyFuture = executorService.scheduleWithFixedDelay(quotaPolicyTask, 0, quotaPolicyTask.getPeriod(), quotaPolicyTask.getPeriodUnit());
        }
        //TODO This doesn't really make sense to log here any more, but is useful to have
        log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Storage check interval: {}ms", config.getQuotaMap(), storageQuotaSoft, storageQuotaHard, storageCheckIntervalMillis);
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }

        Metrics.newGauge(metricName(StorageChecker.class, "TotalStorageUsedBytes"), new Gauge<Long>() {
            public Long value() {
                return storageUsed.get();
            }
        });
        Metrics.newGauge(metricName(StorageChecker.class, "SoftLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return storageQuotaSoft;
            }
        });
        Metrics.newGauge(metricName(StorageChecker.class, "HardLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return storageQuotaHard;
            }
        });
    }

    private void updateUsedStorage(Long newValue) {
        var oldValue = storageUsed.getAndSet(newValue);
        if (oldValue != newValue) {
            resetQuota.set(true);
        }
    }

    private MetricName metricName(Class<?> clazz, String name) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        String mBeanName = String.format("%s:type=%s,name=%s", group, type, name);
        return new MetricName(group, type, name, this.scope, mBeanName);
    }

    private static class ClientQuotaGauge extends Gauge<Double> {
        private final Quota quota;

        public ClientQuotaGauge(Quota quota) {
            this.quota = quota;
        }

        public Double value() {
            return quota.bound();
        }
    }
}
