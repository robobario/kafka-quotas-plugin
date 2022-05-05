/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.types.Volume;

public class ConsumedBytesLimitPolicy implements LimitPolicy {

    private final long limitBytes;

    public ConsumedBytesLimitPolicy(long limitBytes) {
        this.limitBytes = limitBytes;
    }

    @Override
    public boolean breachesLimit(Volume volume) {
        return volume.getConsumed() >= limitBytes;
    }

    @Override
    public Number getLimitValue() {
        return limitBytes;
    }

    @Override
    public long getBreachQuantity(Volume volume) {
        return volume.getConsumed() - limitBytes;
    }
}
