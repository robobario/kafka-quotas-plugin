/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.strimzi.kafka.quotas.QuotaFactorPolicyTask;
import io.strimzi.kafka.quotas.policy.ConsumedBytesLimitPolicy;
import io.strimzi.kafka.quotas.policy.LimitPolicy;
import io.strimzi.kafka.quotas.policy.MinFreeBytesLimitPolicy;
import io.strimzi.kafka.quotas.policy.MinFreePercentageLimitPolicy;
import io.strimzi.kafka.quotas.policy.QuotaFactorPolicy;
import io.strimzi.kafka.quotas.policy.UnlimitedPolicy;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;
import io.strimzi.kafka.quotas.types.Volume;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Determines the active set of brokers on every invocation and calculates the most restrictive quota factor based on the volume usage of those brokers.
 */
public class ActiveBrokerQuotaFactorPolicyTask implements QuotaFactorPolicyTask {
    private final int periodInSeconds;
    private final Supplier<Iterable<VolumeUsageMetrics>> volumeUsageMetricsSupplier;
    private final Supplier<Collection<String>> activeBrokerIdsSupplier;
    private final double missingDataQuotaFactor;
    private final Duration metricsStaleAfter;
    private final Set<Consumer<UpdateQuotaFactor>> updateListeners;
    private final AtomicReference<UpdateQuotaFactor> latestUpdate;
    private final ConcurrentMap<String, VolumeUsageMetrics> mostRecentMetricsPerBroker;

    private final Logger log = getLogger(ActiveBrokerQuotaFactorPolicyTask.class);

    public ActiveBrokerQuotaFactorPolicyTask(int periodInSeconds, Supplier<Iterable<VolumeUsageMetrics>> volumeUsageMetricsSupplier, Supplier<Collection<String>> activeBrokerIdsSupplier, double missingDataQuotaFactor, Duration metricsStaleAfter) {
        this.periodInSeconds = periodInSeconds;
        this.volumeUsageMetricsSupplier = volumeUsageMetricsSupplier;
        this.activeBrokerIdsSupplier = activeBrokerIdsSupplier;
        this.missingDataQuotaFactor = missingDataQuotaFactor;
        this.metricsStaleAfter = metricsStaleAfter;
        this.mostRecentMetricsPerBroker = new ConcurrentHashMap<>();
        log.info("Checking active broker usage: every {} {}", periodInSeconds, getPeriodUnit());
        updateListeners = new CopyOnWriteArraySet<>();
        latestUpdate = new AtomicReference<>();
    }

    @Override
    public long getPeriod() {
        return periodInSeconds;
    }

    @Override
    public TimeUnit getPeriodUnit() {
        return TimeUnit.SECONDS;
    }

    @Override
    public void addListener(Consumer<UpdateQuotaFactor> quotaFactorConsumer) {
        updateListeners.add(quotaFactorConsumer);
        final UpdateQuotaFactor latestUpdated = latestUpdate.get();
        if (latestUpdated != null) {
            quotaFactorConsumer.accept(latestUpdated);
        }
    }

    @Override
    public void run() {
        try {
            updateMetricsCache();
            double quotaRemaining = 1.0;
            final Collection<String> activeBrokers = activeBrokerIdsSupplier.get();
            log.info("checking volume usage for brokerIds: {}", activeBrokers);
            final Instant metricsValidAfter = Instant.now().minus(metricsStaleAfter);
            for (String brokerId : activeBrokers) {
                final VolumeUsageMetrics brokerSnapshot = mostRecentMetricsPerBroker.get(brokerId);
                if (brokerSnapshot == null) {
                    log.warn("no metrics found for {} setting quotaFactor to {}", brokerId, missingDataQuotaFactor);
                    log.debug("no metrics found for {} in {}", brokerId, mostRecentMetricsPerBroker);
                    quotaRemaining = missingDataQuotaFactor;
                    break;
                }
                if (brokerSnapshot.getSnapshotAt().isBefore(metricsValidAfter)) {
                    log.warn("Stale metrics found for {} setting quotaFactor to {}", brokerId, missingDataQuotaFactor);
                    log.debug("Stale metrics found metrics found for {} in {}", brokerId, mostRecentMetricsPerBroker);
                    quotaRemaining = missingDataQuotaFactor;
                    break;
                }
                final QuotaFactorPolicy quotaFactorPolicy = mapLimitsToQuotaPolicy(brokerSnapshot);
                for (Volume volume : brokerSnapshot.getVolumes()) {
                    if (quotaFactorPolicy.breachesHardLimit(volume)) {
                        quotaRemaining = quotaFactorPolicy.quotaFactor(volume);
                        log.warn("hard limit breach for broker-{}:{} setting quotaFactor to 0.0", brokerId, volume.getVolumeName());
                        break;
                    }
                    quotaRemaining = Math.min(quotaRemaining, quotaFactorPolicy.quotaFactor(volume));
                }
            }
            double updatedQuotaFactor = quotaRemaining;
            log.info("Broker volume usage check complete applying quota factor: {}", updatedQuotaFactor);
            final UpdateQuotaFactor updateMessage = new UpdateQuotaFactor(Instant.now(), updatedQuotaFactor);
            latestUpdate.set(updateMessage);
            for (Consumer<UpdateQuotaFactor> updateListener : updateListeners) {
                updateListener.accept(updateMessage);
            }
        } catch (Exception e) {
            log.warn("Error during ActiveBrokerQuotaFactorPolicyTask execution: {}", e.getMessage(), e);
        }
    }

    private void updateMetricsCache() {
        for (VolumeUsageMetrics metricsUpdate : volumeUsageMetricsSupplier.get()) {
            log.debug("checking if {} is current", metricsUpdate);
            //Use putIfAbsent & replace to ensure we don't consider stale data
            final VolumeUsageMetrics lastKnown = mostRecentMetricsPerBroker.putIfAbsent(metricsUpdate.getBrokerId(), metricsUpdate);
            if (lastKnown != null && metricsUpdate.getSnapshotAt().isAfter(lastKnown.getSnapshotAt())) {
                //TODO handle replace == false
                mostRecentMetricsPerBroker.replace(metricsUpdate.getBrokerId(), lastKnown, metricsUpdate);
            } else if (lastKnown != null) {
                log.warn("ignoring volumeMetrics as they are too old: {} currently using snapshot from {}", metricsUpdate.getSnapshotAt(), lastKnown.getSnapshotAt());
            }
        }
    }

    QuotaFactorPolicy mapLimitsToQuotaPolicy(VolumeUsageMetrics usageMetrics) {
        LimitPolicy softLimitPolicy = mapToLimitPolicy(usageMetrics.getSoftLimit());
        LimitPolicy hardLimitPolicy = mapToLimitPolicy(usageMetrics.getHardLimit());
        return new CombinedQuotaFactorPolicy(softLimitPolicy, hardLimitPolicy);
    }

    private LimitPolicy mapToLimitPolicy(Limit limit) {
        switch (limit.getLimitType()) {
            case CONSUMED_BYTES:
                return new ConsumedBytesLimitPolicy(limit.getLevel());
            case MIN_FREE_BYTES:
                return new MinFreeBytesLimitPolicy(limit.getLevel());
            case MIN_FREE_PERCENTAGE:
                return new MinFreePercentageLimitPolicy(limit.getLevel());
            default:
                return UnlimitedPolicy.INSTANCE;
        }
    }
}
