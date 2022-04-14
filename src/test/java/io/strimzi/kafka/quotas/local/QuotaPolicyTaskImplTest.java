/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.time.Instant;
import java.util.List;

import io.strimzi.kafka.quotas.TestUtils;
import io.strimzi.kafka.quotas.policy.ConsumedBytesLimitPolicy;
import io.strimzi.kafka.quotas.policy.QuotaPolicy;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.Volume;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QuotaPolicyTaskImplTest {

    public static final long SOFT_LIMIT = 5L;
    public static final long HARD_LIMIT = 10L;
    private QuotaPolicyTaskImpl quotaPolicyTask;
    private VolumeUsageMetrics volumeUsageMetrics;

    @BeforeEach
    void setUp() {
        volumeUsageMetrics = generateUsageMetrics(TestUtils.newVolumeWith(8L));
        quotaPolicyTask = new QuotaPolicyTaskImpl(10, () -> List.of(volumeUsageMetrics));
    }

    @Test
    void shouldUpdateListener() {
        //Given
        final double expectedFactor = 0.4;
        final Double[] updates = new Double[1];
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifyOfHardLimitBreach() {
        //Given
        volumeUsageMetrics = generateUsageMetrics(TestUtils.newVolumeWith(HARD_LIMIT));
        final double expectedFactor = 0.0;
        final Double[] updates = new Double[1];
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifyOfHardLimitBreachAcrossMultipleBrokers() {
        //Given
        final VolumeUsageMetrics brokerAMetrics = generateUsageMetrics(TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final VolumeUsageMetrics brokerBMetrics = generateUsageMetrics(TestUtils.newVolumeWith(5L), TestUtils.newVolumeWith(HARD_LIMIT));
        final double expectedFactor = 0.0;
        final Double[] updates = new Double[1];
        quotaPolicyTask = new QuotaPolicyTaskImpl(10, () -> List.of(brokerAMetrics, brokerBMetrics));
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifySmallestQuotaFactor() {
        //Given
        volumeUsageMetrics = generateUsageMetrics(TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(8L));
        final double expectedFactor = 0.4;
        final Double[] updates = new Double[1];
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifySmallestQuotaFactorAcrossMultipleBrokers() {
        //Given
        final VolumeUsageMetrics brokerAMetrics = generateUsageMetrics(TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final VolumeUsageMetrics brokerBMetrics = generateUsageMetrics(TestUtils.newVolumeWith(7L), TestUtils.newVolumeWith(8L));
        final double expectedFactor = 0.4;
        final Double[] updates = new Double[1];
        quotaPolicyTask = new QuotaPolicyTaskImpl(10, () -> List.of(brokerAMetrics, brokerBMetrics));
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldMpaUsageMetricsToQuotaPolicy() {
        //Given

        //When
        final QuotaPolicy actualQuotaPolicy = quotaPolicyTask.mapLimitsToQuotaPolicy(volumeUsageMetrics);

        //Then
        assertThat(actualQuotaPolicy).isInstanceOf(CombinedQuotaPolicy.class);
        assertThat(actualQuotaPolicy).extracting("softLimitPolicy").isInstanceOf(ConsumedBytesLimitPolicy.class);
        assertThat(actualQuotaPolicy).extracting("hardLimitPolicy").isInstanceOf(ConsumedBytesLimitPolicy.class);
        assertThat(actualQuotaPolicy.getSoftLimit()).isEqualTo(SOFT_LIMIT);
        assertThat(actualQuotaPolicy.getHardLimit()).isEqualTo(HARD_LIMIT);
    }

    private VolumeUsageMetrics generateUsageMetrics(Volume... volumes) {
        return new VolumeUsageMetrics("-1", Instant.now(), new Limit(Limit.LimitType.CONSUMED_BYTES, HARD_LIMIT), new Limit(Limit.LimitType.CONSUMED_BYTES, SOFT_LIMIT), List.of(volumes));
    }
}
