/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.SortedMap;
import java.util.function.Consumer;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.TestUtils;
import io.strimzi.kafka.quotas.policy.ConsumedBytesLimitPolicy;
import io.strimzi.kafka.quotas.policy.QuotaFactorPolicy;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.UpdateQuotaFactor;
import io.strimzi.kafka.quotas.types.Volume;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.strimzi.kafka.quotas.StaticQuotaCallback.METRICS_SCOPE;
import static io.strimzi.kafka.quotas.TestUtils.assertCounterMetric;
import static io.strimzi.kafka.quotas.TestUtils.getMetricGroup;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ActiveBrokerQuotaFactorPolicyTaskTest {

    private static final long SOFT_LIMIT = 5L;
    private static final long HARD_LIMIT = 10L;
    private static final double MISSING_DATA_QUOTA_FACTOR = 0.0;
    private final Double[] updates = new Double[1];
    private List<String> activeBrokers;
    private ActiveBrokerQuotaFactorPolicyTask quotaPolicyTask;
    private List<VolumeUsageMetrics> currentMetrics;

    @BeforeEach
    void setUp() {
        activeBrokers = List.of("1");
        currentMetrics = List.of(generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(8L)));
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> currentMetrics, () -> activeBrokers, MISSING_DATA_QUOTA_FACTOR, Duration.ofHours(1));
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());
    }

    @AfterEach
    void tearDown() {
        Metrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(m -> METRICS_SCOPE.equals(m.getScope()))
                .forEach(Metrics.defaultRegistry()::removeMetric);
    }

    @Test
    void shouldUpdateListener() {
        //Given
        final double expectedFactor = 0.4;

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifyOfHardLimitBreach() {
        //Given
        currentMetrics = List.of(generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(HARD_LIMIT)));

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(MISSING_DATA_QUOTA_FACTOR));
    }

    @Test
    void shouldNotifyOfHardLimitBreachAcrossMultipleBrokers() {
        //Given
        activeBrokers = List.of("1", "2");
        currentMetrics = List.of(
                generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L)),
                generateUsageMetrics("2", Instant.now(), TestUtils.newVolumeWith(5L), TestUtils.newVolumeWith(HARD_LIMIT)));

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(MISSING_DATA_QUOTA_FACTOR));
    }

    @Test
    void shouldNotifySmallestQuotaFactor() {
        //Given
        final double expectedFactor = 0.4;
        currentMetrics = List.of(generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(8L)));

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifySmallestQuotaFactorAcrossMultipleBrokers() {
        //Given
        final double expectedFactor = 0.4;

        activeBrokers = List.of("1", "2");
        currentMetrics = List.of(
                generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L)),
                generateUsageMetrics("2", Instant.now(), TestUtils.newVolumeWith(7L), TestUtils.newVolumeWith(8L)));

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldTrackUsageForBrokerBetweenUpdates() {
        //Given
        final double expectedFactor = 0.4;
        activeBrokers = List.of("1", "2");
        currentMetrics = List.of();
        final VolumeUsageMetrics broker1Metrics = generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(7L), TestUtils.newVolumeWith(8L));
        final VolumeUsageMetrics broker2Metrics = generateUsageMetrics("2", Instant.now(), TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> currentMetrics, () -> activeBrokers, MISSING_DATA_QUOTA_FACTOR, Duration.ofHours(1));

        //Notify the first brokers metrics
        currentMetrics = List.of(broker1Metrics);
        quotaPolicyTask.run();

        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //Notify the second brokers metrics
        currentMetrics = List.of(broker2Metrics);

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifyConfiguredQuotaForMissingBroker() {
        //Given
        final double expectedFactor = 0.1;
        activeBrokers = List.of("1", "2");
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> currentMetrics, () -> activeBrokers, 0.1, Duration.ofHours(1));
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldIncrementMissingStateCounterForMissingBroker() {
        //Given
        activeBrokers = List.of("1", "2");
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> currentMetrics, () -> activeBrokers, 0.1, Duration.ofHours(1));
        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "ActiveBrokerQuotaFactorPolicyTask");

        //When
        quotaPolicyTask.run();

        //Then
        assertCounterMetric(group, "MissingState", 1L);
    }

    @Test
    void shouldNotifyHardLimitBreachEvenWithMissingBroker() {
        //Given
        final double expectedFactor = 0.0;
        activeBrokers = List.of("1", "2");
        currentMetrics = List.of(generateUsageMetrics("2", Instant.now(), TestUtils.newVolumeWith(10L), TestUtils.newVolumeWith(7L)));

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifySmallestFactorEvenWithMissingBroker() {
        //Given
        final double expectedFactor = 0.8;
        activeBrokers = List.of("1", "2", "3");
        currentMetrics = List.of(
                generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(4L)),
                generateUsageMetrics("3", Instant.now(), TestUtils.newVolumeWith(SOFT_LIMIT + 1)));

        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> currentMetrics, () -> activeBrokers, 1.0, Duration.ofHours(1));
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldNotifyConfiguredQuotaForStaleBroker() {
        //Given
        final VolumeUsageMetrics broker1Metrics = generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final VolumeUsageMetrics broker2Metrics = generateUsageMetrics("2", Instant.now().minus(1, ChronoUnit.HOURS), TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));

        final double expectedFactor = 0.1;
        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> List.of(broker1Metrics, broker2Metrics), () -> List.of("1", "2"), 0.1, Duration.ofHours(1));
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //When
        quotaPolicyTask.run();

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));
    }

    @Test
    void shouldIncrementMissingStateCounterForStaleBroker() {
        //Given
        final VolumeUsageMetrics broker1Metrics = generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));
        final VolumeUsageMetrics broker2Metrics = generateUsageMetrics("2", Instant.now().minus(1, ChronoUnit.HOURS), TestUtils.newVolumeWith(6L), TestUtils.newVolumeWith(7L));

        quotaPolicyTask = new ActiveBrokerQuotaFactorPolicyTask(10, () -> List.of(broker1Metrics, broker2Metrics), () -> List.of("1", "2"), MISSING_DATA_QUOTA_FACTOR, Duration.ofHours(1));
        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "ActiveBrokerQuotaFactorPolicyTask");

        //When
        quotaPolicyTask.run();

        //Then
        assertCounterMetric(group, "MissingState", 1L);
    }

    @Test
    void shouldMapUsageMetricsToQuotaPolicy() {
        //Given
        final VolumeUsageMetrics volumeUsageMetrics = generateUsageMetrics("1", Instant.now(), TestUtils.newVolumeWith(8L));

        //When
        final QuotaFactorPolicy actualQuotaFactorPolicy = quotaPolicyTask.mapLimitsToQuotaPolicy(volumeUsageMetrics);

        //Then
        assertThat(actualQuotaFactorPolicy).isInstanceOf(CombinedQuotaFactorPolicy.class);
        assertThat(actualQuotaFactorPolicy).extracting("softLimitPolicy").isInstanceOf(ConsumedBytesLimitPolicy.class);
        assertThat(actualQuotaFactorPolicy).extracting("hardLimitPolicy").isInstanceOf(ConsumedBytesLimitPolicy.class);
        assertThat(actualQuotaFactorPolicy.getSoftLimit()).isEqualTo(SOFT_LIMIT);
        assertThat(actualQuotaFactorPolicy.getHardLimit()).isEqualTo(HARD_LIMIT);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldOnlyNotifyEachListenerOnce() {
        //Given
        final Consumer<UpdateQuotaFactor> updateQuotaFactorConsumer = mock(Consumer.class);
        quotaPolicyTask.addListener(updateQuotaFactorConsumer);
        quotaPolicyTask.addListener(updateQuotaFactorConsumer);

        //When
        quotaPolicyTask.run();

        //Then
        verify(updateQuotaFactorConsumer, new Times(1)).accept(any());
    }

    @Test
    void shouldNotifyListenerOnLateJoin() {
        //Given
        final double expectedFactor = 0.4;
        quotaPolicyTask.run();

        //When
        quotaPolicyTask.addListener(updateQuotaFactor -> updates[0] = updateQuotaFactor.getFactor());

        //Then
        assertThat(updates).hasSameElementsAs(List.of(expectedFactor));

    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldNotNotifyListenerOnLateJoinIfRunNotInvoked() {
        //Given
        final Consumer<UpdateQuotaFactor> quotaFactorConsumer = mock(Consumer.class);

        //When
        quotaPolicyTask.addListener(quotaFactorConsumer);

        //Then
        Mockito.verifyNoInteractions(quotaFactorConsumer);
    }

    private VolumeUsageMetrics generateUsageMetrics(String brokerId, Instant snapshotAt, Volume... volumes) {
        return new VolumeUsageMetrics(brokerId, snapshotAt, new Limit(Limit.LimitType.CONSUMED_BYTES, HARD_LIMIT), new Limit(Limit.LimitType.CONSUMED_BYTES, SOFT_LIMIT), List.of(volumes));
    }

}
