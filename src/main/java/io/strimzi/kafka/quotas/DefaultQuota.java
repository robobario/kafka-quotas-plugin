/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.server.quota.ClientQuotaEntity;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class DefaultQuota {

    //TODO convert from client-quota-entity to something in the plugin's control so we know it's a safe map key
    private final ConcurrentHashMap<ClientQuotaEntity, Quota> overriddenQuotas = new ConcurrentHashMap<>();

    public Map<String, String> quotaMetricTags(KafkaPrincipal principal, String clientId) {
        String sanitizedUser = Sanitizer.sanitize(principal.getName());
        if (hasOverride(matchesUser(sanitizedUser).and(matchesClientId(clientId)))) {
            return metricTags(clientId, sanitizedUser);
        } else if (hasOverride(matchesUser(sanitizedUser).and(isDefaultClientId()))) {
            return metricTags(clientId, sanitizedUser);
        } else if (hasOverride(matchesUser(sanitizedUser).and(noClientId()))) {
            return metricTags("", sanitizedUser);
        } else if (hasOverride(isDefaultUser().and(matchesClientId(clientId)))) {
            return metricTags(clientId, sanitizedUser);
        } else if (hasOverride(isDefaultUser().and(isDefaultClientId()))) {
            return metricTags(clientId, sanitizedUser);
        } else if (hasOverride(isDefaultUser().and(noClientId()))) {
            return metricTags("", sanitizedUser);
        } else {
            return Map.of();
        }
    }

    private static Map<String, String> metricTags(String clientId, String sanitizedUser) {
        return Map.of("user", sanitizedUser, "client-id", clientId);
    }

    //TODO this should be replaced with map lookups
    private boolean hasOverride(Predicate<ClientQuotaEntity> p) {
        return overriddenQuotas.keySet().stream().anyMatch(p);
    }

    Predicate<ClientQuotaEntity> matchesClientId(String clientId) {
        return matches(clientId, ClientQuotaEntity.ConfigEntityType.CLIENT_ID);
    }

    Predicate<ClientQuotaEntity> matchesUser(String username) {
        return matches(username, ClientQuotaEntity.ConfigEntityType.USER);
    }

    Predicate<ClientQuotaEntity> isDefaultUser() {
        return matchesType(ClientQuotaEntity.ConfigEntityType.DEFAULT_USER);
    }

    Predicate<ClientQuotaEntity> isDefaultClientId() {
        return matchesType(ClientQuotaEntity.ConfigEntityType.DEFAULT_CLIENT_ID);
    }

    Predicate<ClientQuotaEntity> noClientId() {
        return entity -> entity.configEntities().stream().noneMatch(configEntity -> configEntity.entityType().equals(ClientQuotaEntity.ConfigEntityType.DEFAULT_CLIENT_ID) || configEntity.entityType().equals(ClientQuotaEntity.ConfigEntityType.CLIENT_ID));
    }

    Predicate<ClientQuotaEntity> noUser() {
        return entity -> entity.configEntities().stream().noneMatch(configEntity -> configEntity.entityType().equals(ClientQuotaEntity.ConfigEntityType.DEFAULT_USER) || configEntity.entityType().equals(ClientQuotaEntity.ConfigEntityType.USER));
    }

    private static Predicate<ClientQuotaEntity> matches(String name, ClientQuotaEntity.ConfigEntityType type) {
        return entity -> entity.configEntities().stream().anyMatch(configEntity -> configEntity.entityType().equals(type) && configEntity.name().equals(name));
    }

    private static Predicate<ClientQuotaEntity> matchesType(ClientQuotaEntity.ConfigEntityType type) {
        return entity -> entity.configEntities().stream().anyMatch(configEntity -> configEntity.entityType().equals(type));
    }

    //TODO this should be replaced with map lookups
    Optional<Quota> get(Predicate<ClientQuotaEntity> predicate) {
        return overriddenQuotas.entrySet().stream().filter(entry -> predicate.test(entry.getKey())).map(Map.Entry::getValue).findFirst();
    }

    public Double quotaLimit(Map<String, String> metricTags) {
        String sanitizedUser = metricTags.get("user");
        String clientId = metricTags.get("client-id");
        Quota quota = null;

        if (sanitizedUser != null && clientId != null) {
            if (sanitizedUser.length() > 0 && clientId.length() > 0) {
                // /config/users/<user>/clients/<client-id>
                quota = get(matchesUser(sanitizedUser).and(matchesClientId(clientId))).orElse(null);
                if (quota == null) {
                    // /config/users/<user>/clients/<default>
                    quota = get(matchesUser(sanitizedUser).and(isDefaultClientId())).orElse(null);
                }
                if (quota == null) {
                    // /config/users/<default>/clients/<client-id>
                    quota = get(isDefaultUser().and(matchesClientId(clientId))).orElse(null);
                }
                if (quota == null) {
                    // /config/users/<default>/clients/<default>
                    quota = get(isDefaultUser().and(isDefaultClientId())).orElse(null);
                }
            } else if (sanitizedUser.length() > 0) {
                // /config/users/<user>
                quota = get(matchesUser(sanitizedUser).and(noClientId())).orElse(null);
                if (quota == null) {
                    // /config/users/<default>
                    quota = get(isDefaultUser().and(noClientId())).orElse(null);
                }
            } else if (clientId.length() > 0) {
                // /config/clients/<client-id>
                quota = get(matchesClientId(clientId).and(noUser())).orElse(null);
                if (quota == null) {
                    // /config/clients/<default>
                    quota = get(isDefaultClientId().and(noUser())).orElse(null);
                }
            }
        }
        return quota == null ? null : quota.bound();
    }

    public void updateQuota(ClientQuotaEntity quotaEntity, double newValue) {
        overriddenQuotas.put(quotaEntity, new Quota(newValue, true));
    }

    public void removeQuota(ClientQuotaEntity quotaEntity) {
        overriddenQuotas.remove(quotaEntity);
    }

}
