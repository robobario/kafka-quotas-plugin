/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.util.Map;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import io.strimzi.kafka.quotas.QuotaSupplier;
import io.strimzi.kafka.quotas.StaticQuotaCallback;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;

import static io.strimzi.kafka.quotas.StaticQuotaCallback.metricName;
import static java.util.Locale.ENGLISH;
import static org.slf4j.LoggerFactory.getLogger;

public class StaticQuotaSupplier implements QuotaSupplier {
    private final Map<ClientQuotaType, Quota> quotaMap;
    private final Logger log = getLogger(StaticQuotaSupplier.class);

    public StaticQuotaSupplier(Map<ClientQuotaType, Quota> quotaMap) {
        this.quotaMap = quotaMap;
        quotaMap.forEach((clientQuotaType, quota) -> {
            String name = clientQuotaType.name().toUpperCase(ENGLISH).charAt(0) + clientQuotaType.name().toLowerCase(ENGLISH).substring(1);
            Metrics.newGauge(metricName(StaticQuotaCallback.class, name), new ClientQuotaGauge(quota));
        });
        log.info("loaded the following quota limits: {}", quotaMap);
    }

    @Override
    public double quotaFor(ClientQuotaType quotaType, Map<String, String> metricTags) {
        return quotaMap.getOrDefault(quotaType, Quota.upperBound(QuotaSupplier.UNLIMITED)).bound();
    }

    private static class ClientQuotaGauge extends Gauge<Double> {
        private final Quota quota;

        public ClientQuotaGauge(Quota quota) {
            this.quota = quota;
        }

        public Double value() {
            return quota.bound();
        }
    }
}
