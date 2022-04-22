/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Map;

import io.strimzi.kafka.quotas.distributed.KafkaClientManager;
import io.strimzi.kafka.quotas.types.Limit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class StaticQuotaConfigTest {

    public static final String TEST_TOPIC = "testTopic";
    @Mock(lenient = true)
    private KafkaClientManager kafkaClientManager;

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
        final StaticQuotaConfig staticQuotaConfig = newStaticQuotaConfig(Map.of("broker.id", "1"));

        //When
        final String actualBrokerId = staticQuotaConfig.getBrokerId();

        //Then
        assertThat(actualBrokerId).isEqualTo("1");
    }

    @Test
    @SetSystemProperty(key = "broker.id", value = "2")
    void shouldFallbackToSystemPropertiesBrokerId() {
        //Given
        final StaticQuotaConfig staticQuotaConfig = newStaticQuotaConfig(Map.of());

        //When
        final String actualBrokerId = staticQuotaConfig.getBrokerId();

        //Then
        assertThat(actualBrokerId).isEqualTo("2");
    }

    @Test
    @ClearSystemProperty(key = "broker.id")
    void shouldUseDefaultBrokerId() {
        //Given
        final StaticQuotaConfig staticQuotaConfig = newStaticQuotaConfig(Map.of());

        //When
        final String actualBrokerId = staticQuotaConfig.getBrokerId();

        //Then
        assertThat(actualBrokerId).isEqualTo("-1");
    }

    private StaticQuotaConfig newStaticQuotaConfig(Map<String, String> config) {
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(config, false);
        staticQuotaConfig.withKafkaClientManager(kafkaClientManager);
        return staticQuotaConfig;
    }

}
