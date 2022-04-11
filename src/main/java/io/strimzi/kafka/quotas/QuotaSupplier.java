/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Map;

import org.apache.kafka.server.quota.ClientQuotaType;

public interface QuotaSupplier {

    double UNLIMITED = Double.MAX_VALUE;
    double PAUSED = 1.0;

    /**
     * For the given quotaType and tags determine what the configured quota is.
     *
     * The quota returned by this call is expected to be stable and represent the best case limit for a given request.
     * If there is no limit to for quota implementations are expected to return {@code io.strimzi.kafka.quotas.QuotaSupplier#UNLIMITED}
     *
     * @param quotaType -  Type of quota requested
     * @param metricTags - Metric tags for a quota metric of type `quotaType`
     * @return the quota in bytes per second
     */
    double quotaFor(ClientQuotaType quotaType, Map<String, String> metricTags);
}
