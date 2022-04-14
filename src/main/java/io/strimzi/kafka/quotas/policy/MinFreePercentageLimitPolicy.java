/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.types.Volume;

public class MinFreePercentageLimitPolicy implements LimitPolicy {
    private final double minFreePercentage;

    //TODO Do we expect min free % to be 0..100 or 0.0..1.0
    public MinFreePercentageLimitPolicy(double minFreePercentage) {
        this.minFreePercentage = minFreePercentage;
    }

    @Override
    public boolean breachesLimit(Volume volume) {
        final long freeSpace = getFreeSpace(volume);
        final long minFreeBytes = getLimitInBytes(volume);
        return minFreeBytes >= freeSpace;
    }

    @Override
    public Number getLimitValue() {
        return minFreePercentage;
    }

    @Override
    public long getBreachQuantity(Volume volume) {
        return getLimitInBytes(volume) -  getFreeSpace(volume);
    }

    private long getLimitInBytes(Volume volume) {
        return Math.round(volume.getCapacity() * minFreePercentage);
    }

    private long getFreeSpace(Volume volume) {
        return volume.getCapacity() - volume.getConsumed();
    }
}
