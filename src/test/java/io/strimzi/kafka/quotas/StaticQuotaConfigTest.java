/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Map;

import io.strimzi.kafka.quotas.types.Limit;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;

class StaticQuotaConfigTest {

    public static final int LIMIT_BYTES = 10000;

    @Test
    void shouldConfigureConsumedBytesSoftLimit() {
        //Given
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(Map.of(StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, LIMIT_BYTES), false);
        final Limit expectedLimit = new Limit(Limit.LimitType.CONSUMED_BYTES, LIMIT_BYTES);

        //When
        Limit actualLimit = staticQuotaConfig.getSoftLimit();

        //Then
        assertThat(actualLimit).isEqualTo(expectedLimit);
    }

    @Test
    void shouldConfigureConsumedBytesHardLimit() {
        //Given
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(Map.of(StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, LIMIT_BYTES), false);
        final Limit expectedLimit = new Limit(Limit.LimitType.CONSUMED_BYTES, LIMIT_BYTES);

        //When
        Limit actualLimit = staticQuotaConfig.getHardLimit();

        //Then
        assertThat(actualLimit).isEqualTo(expectedLimit);
    }

    @Test
    @SetSystemProperty(key = "broker.id", value = "2")
    void shouldUseConfiguredBrokerId() {
        //Given
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(Map.of("broker.id", "1"), false);

        //When
        final String actualBrokerId = staticQuotaConfig.getBrokerId();

        //Then
        assertThat(actualBrokerId).isEqualTo("1");
    }

    @Test
    @SetSystemProperty(key = "broker.id", value = "2")
    void shouldFallbackToSystemPropertiesBrokerId() {
        //Given
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(Map.of(), false);

        //When
        final String actualBrokerId = staticQuotaConfig.getBrokerId();

        //Then
        assertThat(actualBrokerId).isEqualTo("2");
    }

    @Test
    @ClearSystemProperty(key = "broker.id")
    void shouldUseDefaultBrokerId() {
        //Given
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(Map.of(), false);

        //When
        final String actualBrokerId = staticQuotaConfig.getBrokerId();

        //Then
        assertThat(actualBrokerId).isEqualTo("-1");
    }
}
