/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.time.Instant;
import java.util.ArrayList;
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

class ActiveBrokerQuotaPolicyTaskTest {

    public static final long SOFT_LIMIT = 5L;
    public static final long HARD_LIMIT = 10L;
    private ActiveBrokerQuotaPolicyTask quotaPolicyTask;
    private VolumeUsageMetrics volumeUsageMetrics;

    @BeforeEach
    void setUp() {
        volumeUsageMetrics = generateUsageMetrics("-1", TestUtils.newVolumeWith(8L));
        quotaPolicyTask = new ActiveBrokerQuotaPolicyTask(10, () -> List.of(volumeUsageMetrics), () -> List.of("-1"));
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
        volumeUsageMetrics = generateUsageMetrics("-1", TestUtils.newVolumeWith(HARD_LIMIT));
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
        final VolumeUsageMetrics broker1Metrics = generateUsageMetrics("1", TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final VolumeUsageMetrics brokerBMetrics = generateUsageMetrics("2", TestUtils.newVolumeWith(5L), TestUtils.newVolumeWith(HARD_LIMIT));
        final double expectedFactor = 0.0;
        final Double[] updates = new Double[1];
        quotaPolicyTask = new ActiveBrokerQuotaPolicyTask(10, () -> List.of(broker1Metrics, brokerBMetrics), () -> List.of("1", "2"));
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifySmallestQuotaFactor() {
        //Given
        volumeUsageMetrics = generateUsageMetrics("-1", TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(8L));
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
        final VolumeUsageMetrics broker1Metrics = generateUsageMetrics("1", TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final VolumeUsageMetrics broker2Metrics = generateUsageMetrics("2", TestUtils.newVolumeWith(7L), TestUtils.newVolumeWith(8L));
        final double expectedFactor = 0.4;
        final Double[] updates = new Double[1];
        quotaPolicyTask = new ActiveBrokerQuotaPolicyTask(10, () -> List.of(broker1Metrics, broker2Metrics), () -> List.of("1", "2"));
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldTrackUsageForBrokerBetweenUpdates() {
        //Given
        final VolumeUsageMetrics broker1Metrics = generateUsageMetrics("1", TestUtils.newVolumeWith(7L), TestUtils.newVolumeWith(8L));
        final VolumeUsageMetrics broker2Metrics = generateUsageMetrics("2", TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final List<VolumeUsageMetrics> currentMetrics = new ArrayList<>();
        quotaPolicyTask = new ActiveBrokerQuotaPolicyTask(10, () -> currentMetrics, () -> List.of("1", "2"));
        final double expectedFactor = 0.4;
        final Double[] updates = new Double[1];

        //Notify the first brokers metrics
        currentMetrics.add(broker1Metrics);
        quotaPolicyTask.run();

        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //Notify the second brokers metrics
        currentMetrics.clear();
        currentMetrics.add(broker2Metrics);

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifyZeroQuotaForMissingBroker() {
        //Given
        final VolumeUsageMetrics broker1Metrics = generateUsageMetrics("1", TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final double expectedFactor = 0.0;
        final Double[] updates = new Double[1];
        quotaPolicyTask = new ActiveBrokerQuotaPolicyTask(10, () -> List.of(broker1Metrics), () -> List.of("1", "2"));
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

    private VolumeUsageMetrics generateUsageMetrics(String brokerId, Volume... volumes) {
        return new VolumeUsageMetrics(brokerId, Instant.now(), new Limit(Limit.LimitType.CONSUMED_BYTES, HARD_LIMIT), new Limit(Limit.LimitType.CONSUMED_BYTES, SOFT_LIMIT), List.of(volumes));
    }
}
