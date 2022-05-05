/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.types.Volume;

public interface LimitPolicy {

    boolean breachesLimit(Volume volume);

    Number getLimitValue();

    long getBreachQuantity(Volume volume);
}
