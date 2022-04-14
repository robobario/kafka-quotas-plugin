/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.strimzi.kafka.quotas.QuotaPolicyTask;
import io.strimzi.kafka.quotas.policy.ConsumedBytesLimitPolicy;
import io.strimzi.kafka.quotas.policy.LimitPolicy;
import io.strimzi.kafka.quotas.policy.MinFreeBytesLimitPolicy;
import io.strimzi.kafka.quotas.policy.MinFreePercentageLimitPolicy;
import io.strimzi.kafka.quotas.policy.QuotaPolicy;
import io.strimzi.kafka.quotas.policy.UnlimitedPolicy;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;
import io.strimzi.kafka.quotas.types.Volume;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;

public class QuotaPolicyTaskImpl implements QuotaPolicyTask {
    private final int periodInSeconds;
    private final Supplier<Iterable<VolumeUsageMetrics>> volumeUsageMetricsSupplier;
    private final List<Consumer<UpdateQuotaFactor>> updateListeners = new ArrayList<>();

    public QuotaPolicyTaskImpl(int periodInSeconds, Supplier<Iterable<VolumeUsageMetrics>> volumeUsageMetricsSupplier) {
        this.periodInSeconds = periodInSeconds;
        this.volumeUsageMetricsSupplier = volumeUsageMetricsSupplier;
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
        final Iterable<VolumeUsageMetrics> volumeUsageMetrics = volumeUsageMetricsSupplier.get();
        double quotaFactor = 0.0D;
        for (VolumeUsageMetrics brokerSnapshot : volumeUsageMetrics) {
            final QuotaPolicy quotaPolicy = mapLimitsToQuotaPolicy(brokerSnapshot);
            for (Volume volume : brokerSnapshot.getVolumes()) {
                if (quotaPolicy.breachesHardLimit(volume)) {
                    quotaFactor = quotaPolicy.quotaFactor(volume);
                    break;
                }
                quotaFactor = Math.max(quotaFactor, quotaPolicy.quotaFactor(volume));
            }
        }
        for (Consumer<UpdateQuotaFactor> updateListener : updateListeners) {
            updateListener.accept(new UpdateQuotaFactor(Instant.now(), 1.0 - quotaFactor));
        }
    }

    QuotaPolicy mapLimitsToQuotaPolicy(VolumeUsageMetrics usageMetrics) {
        LimitPolicy softLimitPolicy = mapToLimitPolicy(usageMetrics.getSoftLimit());
        LimitPolicy hardLimitPolicy = mapToLimitPolicy(usageMetrics.getHardLimit());
        return new CombinedQuotaPolicy(softLimitPolicy, hardLimitPolicy);
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
