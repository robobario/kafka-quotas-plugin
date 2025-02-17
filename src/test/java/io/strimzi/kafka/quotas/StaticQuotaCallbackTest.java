/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class StaticQuotaCallbackTest {

    public static final Map<String, Integer> MINIMUM_EXECUTABLE_CONFIG = Map.of(StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, 10);

    StaticQuotaCallback target;

    ScheduledExecutorService backgroundScheduler = Executors.newSingleThreadScheduledExecutor();

    @BeforeEach
    void setup() {
        target = new StaticQuotaCallback();
    }

    @AfterEach
    void tearDown() {
        target.close();
    }

    @Test
    void quotaDefaults() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of());

        double produceQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, produceQuotaLimit);

        double fetchQuotaLimit = target.quotaLimit(ClientQuotaType.FETCH, target.quotaMetricTags(ClientQuotaType.FETCH, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, fetchQuotaLimit);
    }

    @Test
    void produceQuota() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024));

        double quotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(1024, quotaLimit);
    }

    @Test
    void excludedPrincipal() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(StaticQuotaConfig.EXCLUDED_PRINCIPAL_NAME_LIST_PROP, "foo,bar",
                                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024));
        double fooQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, fooQuotaLimit);

        KafkaPrincipal baz = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "baz");
        double bazQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, baz, "clientId"));
        assertEquals(1024, bazQuotaLimit);
    }

    @Test
    void shouldScheduleStorageChecker() {
        //Given
        StorageChecker storageChecker = mock(StorageChecker.class);
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(storageChecker, scheduledExecutorService);

        //When
        target.configure(Map.of(StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "1"));

        //Verify
        verify(scheduledExecutorService, times(1)).scheduleWithFixedDelay(any(), eq(1000L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldNotScheduleStorageCheckWhenCheckIntervalIsZero() {
        //Given
        StorageChecker storageChecker = mock(StorageChecker.class);
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(storageChecker, scheduledExecutorService);

        //When
        target.configure(Map.of(StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "0"));

        //Then
        verify(scheduledExecutorService, times(0)).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void shouldNotScheduleStorageCheckWhenCheckIntervalIsNotProvided() {
        //Given
        StorageChecker storageChecker = mock(StorageChecker.class);
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(storageChecker, scheduledExecutorService);

        //When
        target.configure(Map.of());

        //Then
        verify(scheduledExecutorService, times(0)).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void shouldShutdownExecutorOnClose() {
        //Given
        StorageChecker storageChecker = mock(StorageChecker.class);
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(storageChecker, scheduledExecutorService);
        target.configure(MINIMUM_EXECUTABLE_CONFIG);

        //When
        target.close();

        //Verify
        verify(scheduledExecutorService, times(1)).shutdownNow();
    }

    @SuppressWarnings("unchecked")
    @Test
    void quotaResetRequiredShouldRespectQuotaType() {
        StorageChecker mock = mock(StorageChecker.class);
        ArgumentCaptor<Consumer<Long>> argument = ArgumentCaptor.forClass(Consumer.class);
        doNothing().when(mock).configure(anyList(), argument.capture());
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(mock, backgroundScheduler);
        quotaCallback.configure(MINIMUM_EXECUTABLE_CONFIG);
        Consumer<Long> storageUpdateConsumer = argument.getValue();
        quotaCallback.updateClusterMetadata(null);

        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH), "unexpected initial state");

        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH), "unexpected state on subsequent call without storage state change");

        //When
        storageUpdateConsumer.accept(1L);

        //Then
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 1st storage state change");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH), "unexpected state on subsequent call after 1st storage state change");

        quotaCallback.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void quotaResetRequired() {
        StorageChecker mock = mock(StorageChecker.class);
        ArgumentCaptor<Consumer<Long>> argument = ArgumentCaptor.forClass(Consumer.class);
        doNothing().when(mock).configure(anyList(), argument.capture());
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(mock, backgroundScheduler);
        quotaCallback.configure(MINIMUM_EXECUTABLE_CONFIG);
        Consumer<Long> storageUpdateConsumer = argument.getValue();
        quotaCallback.updateClusterMetadata(null);

        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        storageUpdateConsumer.accept(1L);
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 1st storage state change");
        storageUpdateConsumer.accept(1L);
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        storageUpdateConsumer.accept(2L);
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 2nd storage state change");

        quotaCallback.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void storageCheckerMetrics() {
        StorageChecker mock = mock(StorageChecker.class);
        ArgumentCaptor<Consumer<Long>> argument = ArgumentCaptor.forClass(Consumer.class);
        doNothing().when(mock).configure(anyList(), argument.capture());

        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(mock, backgroundScheduler);

        quotaCallback.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 15L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 16L,
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, 10
        ));

        argument.getValue().accept(17L);

        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "StorageChecker");

        assertGaugeMetric(group, "SoftLimitBytes", 15L);
        assertGaugeMetric(group, "HardLimitBytes", 16L);
        assertGaugeMetric(group, "TotalStorageUsedBytes", 17L);

        // the mbean name is part of the public api
        MetricName name = group.firstKey();
        String expectedMbeanName = String.format("io.strimzi.kafka.quotas:type=StorageChecker,name=%s", name.getName());
        assertEquals(expectedMbeanName, name.getMBeanName(), "unexpected mbean name");

        quotaCallback.close();
    }

    @Test
    void staticQuotaMetrics() {

        target.configure(Map.of(
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 15.0,
                StaticQuotaConfig.FETCH_QUOTA_PROP, 16.0,
                StaticQuotaConfig.REQUEST_QUOTA_PROP, 17.0
        ));

        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "StaticQuotaCallback");

        assertGaugeMetric(group, "Produce", 15.0);
        assertGaugeMetric(group, "Fetch", 16.0);
        assertGaugeMetric(group, "Request", 17.0);

        // the mbean name is part of the public api
        MetricName name = group.firstKey();
        String expectedMbeanName = String.format("io.strimzi.kafka.quotas:type=StaticQuotaCallback,name=%s", name.getName());
        assertEquals(expectedMbeanName, name.getMBeanName(), "unexpected mbean name");
    }

    private SortedMap<MetricName, Metric> getMetricGroup(String p, String t) {
        SortedMap<String, SortedMap<MetricName, Metric>> storageMetrics = Metrics.defaultRegistry().groupedMetrics((name, metric) -> p.equals(name.getScope()) && t.equals(name.getType()));
        assertEquals(1, storageMetrics.size(), "unexpected number of metrics in group");
        return storageMetrics.entrySet().iterator().next().getValue();
    }

    private <T> void assertGaugeMetric(SortedMap<MetricName, Metric> metrics, String name, T expected) {
        Optional<Gauge<T>> desired = findGaugeMetric(metrics, name);
        assertTrue(desired.isPresent(), String.format("metric with name %s not found in %s", name, metrics));
        Gauge<T> gauge = desired.get();
        assertEquals(expected, gauge.value(), String.format("metric %s has unexpected value", name));
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<Gauge<T>> findGaugeMetric(SortedMap<MetricName, Metric> metrics, String name) {
        return metrics.entrySet().stream().filter(e -> name.equals(e.getKey().getName())).map(e -> (Gauge<T>) e.getValue()).findFirst();
    }
}
