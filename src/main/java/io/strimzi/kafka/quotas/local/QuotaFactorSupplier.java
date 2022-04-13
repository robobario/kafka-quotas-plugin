/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;

public class QuotaFactorSupplier implements io.strimzi.kafka.quotas.QuotaFactorSupplier, Runnable {

    private final Supplier<UpdateQuotaFactor> factorSupplier;
    private final AtomicLong currentFactor = new AtomicLong(Double.doubleToLongBits(1.0));

    public QuotaFactorSupplier(Supplier<UpdateQuotaFactor> factorSupplier) {
        this.factorSupplier = factorSupplier;
    }

    @Override
    public Double get() {
        return Double.longBitsToDouble(currentFactor.get());
    }

    @Override
    public void run() {
        final UpdateQuotaFactor updateQuotaFactor = factorSupplier.get();
        //TODO consider validFrom
        currentFactor.set(Double.doubleToLongBits(updateQuotaFactor.getFactor()));
    }
}
