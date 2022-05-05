/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.util.Map;

import io.strimzi.kafka.quotas.QuotaFactorSupplier;
import io.strimzi.kafka.quotas.QuotaSupplier;
import org.apache.kafka.server.quota.ClientQuotaType;

public class UnlimitedQuotaSupplier implements QuotaSupplier, QuotaFactorSupplier {

    public static final UnlimitedQuotaSupplier UNLIMITED_QUOTA_SUPPLIER = new UnlimitedQuotaSupplier();

    private UnlimitedQuotaSupplier() {
    }

    @Override
    public double quotaFor(ClientQuotaType quotaType, Map<String, String> metricTags) {
        return QuotaSupplier.UNLIMITED;
    }

    @Override
    public Double get() {
        return 1.0;
    }

    @Override
    public void addUpdateListener(Runnable listener) {
        listener.run(); //Run it once to trigger it, but otherwise it will never change.
    }
}
