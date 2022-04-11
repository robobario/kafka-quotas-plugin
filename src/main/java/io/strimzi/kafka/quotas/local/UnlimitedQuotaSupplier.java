/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.util.Map;

import io.strimzi.kafka.quotas.QuotaSupplier;
import org.apache.kafka.server.quota.ClientQuotaType;

public class UnlimitedQuotaSupplier implements QuotaSupplier {

    public static final QuotaSupplier UNLIMITED_QUOTA_SUPPLIER = new UnlimitedQuotaSupplier();

    private UnlimitedQuotaSupplier() {
    }

    @Override
    public double quotaFor(ClientQuotaType quotaType, Map<String, String> metricTags) {
        return QuotaSupplier.UNLIMITED;
    }
}
