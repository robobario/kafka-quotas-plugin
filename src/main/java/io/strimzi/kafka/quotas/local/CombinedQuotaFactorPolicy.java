/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import io.strimzi.kafka.quotas.policy.LimitPolicy;
import io.strimzi.kafka.quotas.policy.QuotaFactorPolicy;
import io.strimzi.kafka.quotas.types.Volume;

public class CombinedQuotaFactorPolicy implements QuotaFactorPolicy {
    private final LimitPolicy softLimitPolicy;
    private final LimitPolicy hardLimitPolicy;

    public CombinedQuotaFactorPolicy(LimitPolicy softLimitPolicy, LimitPolicy hardLimitPolicy) {
        this.softLimitPolicy = softLimitPolicy;
        this.hardLimitPolicy = hardLimitPolicy;
    }

    @Override
    public boolean breachesHardLimit(Volume volumeDetails) {
        return hardLimitPolicy.breachesLimit(volumeDetails);
    }

    @Override
    public boolean breachesSoftLimit(Volume volumeDetails) {
        return softLimitPolicy.breachesLimit(volumeDetails);
    }

    @Override
    public double quotaFactor(Volume volumeDetails) {
        final long softLimit = softLimitPolicy.getLimitValue().longValue();
        final long hardLimit = hardLimitPolicy.getLimitValue().longValue();
        final long overQuotaUsage = softLimitPolicy.getBreachQuantity(volumeDetails);
        final long quotaCapacity = hardLimit - softLimit;
        if (softLimitPolicy.breachesLimit(volumeDetails) && quotaCapacity > 0) {
            return 1.0 - Math.abs(Math.min(1.0, 1.0 * overQuotaUsage / quotaCapacity));
        } else if (hardLimitPolicy.breachesLimit(volumeDetails)) {
            return 0.0;
        } else {
            return 1.0;
        }
    }

    @Override
    public Number getSoftLimit() {
        return softLimitPolicy.getLimitValue();
    }

    @Override
    public Number getHardLimit() {
        return hardLimitPolicy.getLimitValue();
    }
}
