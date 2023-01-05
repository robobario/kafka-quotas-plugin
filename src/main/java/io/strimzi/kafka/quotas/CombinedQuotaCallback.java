/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;

import java.util.Map;

public class CombinedQuotaCallback implements ClientQuotaCallback {

    private final StaticQuotaCallback staticQuotaCallback;
    private final DefaultQuotaCallback defaultQuotaCallback;

    public CombinedQuotaCallback() {
        staticQuotaCallback = new StaticQuotaCallback();
        defaultQuotaCallback = new DefaultQuotaCallback();
    }

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType clientQuotaType, KafkaPrincipal kafkaPrincipal, String s) {
        Map<String, String> defaultTags = defaultQuotaCallback.quotaMetricTags(clientQuotaType, kafkaPrincipal, s);
        if (!defaultTags.isEmpty()) {
            return defaultTags;
        } else {
            return staticQuotaCallback.quotaMetricTags(clientQuotaType, kafkaPrincipal, s);
        }
    }

    @Override
    public Double quotaLimit(ClientQuotaType clientQuotaType, Map<String, String> map) {
        if (map.containsKey("user")) {
            return defaultQuotaCallback.quotaLimit(clientQuotaType, map);
        } else {
            return staticQuotaCallback.quotaLimit(clientQuotaType, map);
        }
    }

    @Override
    public void updateQuota(ClientQuotaType clientQuotaType, ClientQuotaEntity clientQuotaEntity, double v) {
        defaultQuotaCallback.updateQuota(clientQuotaType, clientQuotaEntity, v);
    }

    @Override
    public void removeQuota(ClientQuotaType clientQuotaType, ClientQuotaEntity clientQuotaEntity) {
        defaultQuotaCallback.removeQuota(clientQuotaType, clientQuotaEntity);
    }

    @Override
    public boolean quotaResetRequired(ClientQuotaType clientQuotaType) {
        return staticQuotaCallback.quotaResetRequired(clientQuotaType);
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        return false;
    }

    @Override
    public void close() {
        staticQuotaCallback.close();
    }

    @Override
    public void configure(Map<String, ?> map) {
        staticQuotaCallback.configure(map);
    }
}
