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
import java.util.concurrent.ConcurrentHashMap;

public class DefaultQuotaCallback implements ClientQuotaCallback {
    private final ConcurrentHashMap<ClientQuotaType, DefaultQuota> overriddenQuotas = new ConcurrentHashMap<>();


    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        return getQuota(quotaType).quotaMetricTags(principal, clientId);
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        return getQuota(quotaType).quotaLimit(metricTags);
    }

    @Override
    public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue) {
        getQuota(quotaType).updateQuota(quotaEntity, newValue);
    }

    @Override
    public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
        getQuota(quotaType).removeQuota(quotaEntity);
    }

    private DefaultQuota getQuota(ClientQuotaType quotaType) {
        return overriddenQuotas.computeIfAbsent(quotaType, type -> new DefaultQuota());
    }

    @Override
    public boolean quotaResetRequired(ClientQuotaType quotaType) {
        return false;
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
