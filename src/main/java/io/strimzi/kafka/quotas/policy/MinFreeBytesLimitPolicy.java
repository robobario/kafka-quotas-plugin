/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.types.Volume;

public class MinFreeBytesLimitPolicy implements LimitPolicy {

    private final long minFreeBytes;

    public MinFreeBytesLimitPolicy(long minFreeBytes) {
        this.minFreeBytes = minFreeBytes;
    }

    @Override
    public boolean breachesLimit(Volume volume) {
        final long freeSpace = getFreeSpace(volume);
        return minFreeBytes >= freeSpace;
    }

    @Override
    public Number getLimitValue() {
        return minFreeBytes;
    }

    @Override
    public long getBreachQuantity(Volume volume) {
        return minFreeBytes - getFreeSpace(volume);
    }

    private long getFreeSpace(Volume volume) {
        return volume.getCapacity() - volume.getConsumed();
    }
}
