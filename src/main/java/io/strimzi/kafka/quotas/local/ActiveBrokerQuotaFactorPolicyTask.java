/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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
    private final List<Consumer<UpdateQuotaFactor>> updateListeners = new ArrayList<>();

    private final ConcurrentMap<String, VolumeUsageMetrics> mostRecentMetricsPerBroker;

    private final Logger log = getLogger(ActiveBrokerQuotaFactorPolicyTask.class);

    public ActiveBrokerQuotaFactorPolicyTask(int periodInSeconds, Supplier<Iterable<VolumeUsageMetrics>> volumeUsageMetricsSupplier, Supplier<Collection<String>> activeBrokerIdsSupplier) {
        this.periodInSeconds = periodInSeconds;
        this.volumeUsageMetricsSupplier = volumeUsageMetricsSupplier;
        this.activeBrokerIdsSupplier = activeBrokerIdsSupplier;
        mostRecentMetricsPerBroker = new ConcurrentHashMap<>();
        log.info("Checking active broker usage: every {} {}", periodInSeconds, getPeriodUnit());
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
    }

    @Override
    public void run() {
        updateMetricsCache();
        double quotaRemaining = 1.0D;
        final Collection<String> activeBrokers = activeBrokerIdsSupplier.get();
        log.info("checking volume usage for brokerIds: {}", activeBrokers);
        for (String brokerId : activeBrokers) {
            final VolumeUsageMetrics brokerSnapshot = mostRecentMetricsPerBroker.get(brokerId);
            if (brokerSnapshot == null) {
                log.warn("no metrics found for {} setting quotaFactor to 0.0", brokerId);
                log.debug("no metrics found for {} in {}", brokerId, mostRecentMetricsPerBroker);
                quotaRemaining = 0.0D;
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
        for (Consumer<UpdateQuotaFactor> updateListener : updateListeners) {
            updateListener.accept(updateMessage);
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
