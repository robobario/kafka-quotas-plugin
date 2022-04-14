/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import io.strimzi.kafka.quotas.QuotaFactorSupplier;
import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;

public class InMemoryQuotaFactorSupplier implements QuotaFactorSupplier, Consumer<UpdateQuotaFactor> {
    private final AtomicLong currentFactor = new AtomicLong();

    @Override
    public Double get() {
        return Double.longBitsToDouble(currentFactor.get());
    }

    @Override
    public void accept(UpdateQuotaFactor updateQuotaFactor) {
        final double newFactor = updateQuotaFactor.getFactor();
        currentFactor.set(Double.doubleToLongBits(newFactor));
    }
}
