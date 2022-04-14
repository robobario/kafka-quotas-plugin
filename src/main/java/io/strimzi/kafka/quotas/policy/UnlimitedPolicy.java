/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.types.Volume;

public class UnlimitedPolicy implements LimitPolicy {

    public static final UnlimitedPolicy INSTANCE = new UnlimitedPolicy();

    private UnlimitedPolicy() {
    }

    @Override
    public boolean breachesLimit(Volume volume) {
        return false;
    }

    @Override
    public Number getLimitValue() {
        return 0;
    }

    @Override
    public long getBreachQuantity(Volume volume) {
        return 0L;
    }
}
