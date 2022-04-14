/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.strimzi.kafka.quotas.QuotaPolicyTask;
import io.strimzi.kafka.quotas.policy.ConsumedBytesLimitPolicy;
import io.strimzi.kafka.quotas.policy.LimitPolicy;
import io.strimzi.kafka.quotas.policy.QuotaPolicy;
import io.strimzi.kafka.quotas.policy.UnlimitedPolicy;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.Volume;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;

public class QuotaPolicyTaskImpl implements QuotaPolicyTask {
    private final int periodInSeconds;
    private final Supplier<Iterable<VolumeUsageMetrics>> volumeUsageMetricsSupplier;
    private final List<Consumer<Double>> updateListeners = new ArrayList<>();

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
    public void addListener(Consumer<Double> quotaFactorConsumer) {
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
        for (Consumer<Double> updateListener : updateListeners) {
            updateListener.accept(1.0 - quotaFactor);
        }
    }

    QuotaPolicy mapLimitsToQuotaPolicy(VolumeUsageMetrics usageMetrics) {
        final Limit softLimit = usageMetrics.getSoftLimit();
        final Limit hardLimit = usageMetrics.getHardLimit();
        LimitPolicy softLimitPolicy = mapToLimitPolicy(softLimit);
        LimitPolicy hardLimitPolicy = mapToLimitPolicy(hardLimit);
        return new CombinedQuotaPolicy(softLimitPolicy, hardLimitPolicy);
    }

    private LimitPolicy mapToLimitPolicy(Limit limit) {
        switch (limit.getLimitType()) {
            case CONSUMED_BYTES:
                return new ConsumedBytesLimitPolicy(limit.getLevel());
            default:
                return UnlimitedPolicy.INSTANCE;
        }
    }
}
