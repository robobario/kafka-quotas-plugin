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
import io.strimzi.kafka.quotas.policy.QuotaFactorPolicy;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.Volume;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ActiveBrokerQuotaFactorPolicyTaskTest {

    private static final long SOFT_LIMIT = 5L;
    private static final long HARD_LIMIT = 10L;
    private static final double MISSING_DATA_QUOTA_FACTOR = 0.0;
    private final Double[] updates = new Double[1];
    private ActiveBrokerQuotaFactorPolicyTask quotaPolicyTask;
    private VolumeUsageMetrics volumeUsageMetrics;

    @BeforeEach
    void setUp() {
        volumeUsageMetrics = generateUsageMetrics("-1", TestUtils.newVolumeWith(8L));
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> List.of(volumeUsageMetrics), () -> List.of("-1"), MISSING_DATA_QUOTA_FACTOR);
    }

    @Test
    void shouldUpdateListener() {
        //Given
        final double expectedFactor = 0.4;
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
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(MISSING_DATA_QUOTA_FACTOR));
    }

    @Test
    void shouldNotifyOfHardLimitBreachAcrossMultipleBrokers() {
        //Given
        final VolumeUsageMetrics broker1Metrics = generateUsageMetrics("1", TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final VolumeUsageMetrics brokerBMetrics = generateUsageMetrics("2", TestUtils.newVolumeWith(5L), TestUtils.newVolumeWith(HARD_LIMIT));
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> List.of(broker1Metrics, brokerBMetrics), () -> List.of("1", "2"), MISSING_DATA_QUOTA_FACTOR);
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(MISSING_DATA_QUOTA_FACTOR));
    }

    @Test
    void shouldNotifySmallestQuotaFactor() {
        //Given
        volumeUsageMetrics = generateUsageMetrics("-1", TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(8L));
        final double expectedFactor = 0.4;
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
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> List.of(broker1Metrics, broker2Metrics), () -> List.of("1", "2"), MISSING_DATA_QUOTA_FACTOR);
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
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> currentMetrics, () -> List.of("1", "2"), MISSING_DATA_QUOTA_FACTOR);
        final double expectedFactor = 0.4;

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
    void shouldNotifyConfiguredQuotaForMissingBroker() {
        //Given
        final VolumeUsageMetrics broker1Metrics = generateUsageMetrics("1", TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final double expectedFactor = 0.1;
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> List.of(broker1Metrics), () -> List.of("1", "2"), 0.1);
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
        final QuotaFactorPolicy actualQuotaFactorPolicy = quotaPolicyTask.mapLimitsToQuotaPolicy(volumeUsageMetrics);

        //Then
        assertThat(actualQuotaFactorPolicy).isInstanceOf(CombinedQuotaFactorPolicy.class);
        assertThat(actualQuotaFactorPolicy).extracting("softLimitPolicy").isInstanceOf(ConsumedBytesLimitPolicy.class);
        assertThat(actualQuotaFactorPolicy).extracting("hardLimitPolicy").isInstanceOf(ConsumedBytesLimitPolicy.class);
        assertThat(actualQuotaFactorPolicy.getSoftLimit()).isEqualTo(SOFT_LIMIT);
        assertThat(actualQuotaFactorPolicy.getHardLimit()).isEqualTo(HARD_LIMIT);
    }

    private VolumeUsageMetrics generateUsageMetrics(String brokerId, Volume... volumes) {
        return new VolumeUsageMetrics(brokerId, Instant.now(), new Limit(Limit.LimitType.CONSUMED_BYTES, HARD_LIMIT), new Limit(Limit.LimitType.CONSUMED_BYTES, SOFT_LIMIT), List.of(volumes));
    }
}
