/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StaticQuotaSupplierTest {

    public static final double PRODUCE_QUOTA = 1024.0;
    public static final double FETCH_QUOTA = 456.0;
    public static final double REQUEST_QUOTA = 123456.0;
    public static final double EPSILON = 0.00001;
    private StaticQuotaSupplier staticQuotaSupplier;

    @BeforeEach
    void setUp() {
        staticQuotaSupplier = new StaticQuotaSupplier(Map.of(
                ClientQuotaType.PRODUCE, Quota.upperBound(PRODUCE_QUOTA),
                ClientQuotaType.FETCH, Quota.upperBound(FETCH_QUOTA),
                ClientQuotaType.REQUEST, Quota.upperBound(REQUEST_QUOTA)));
    }

    @Test
    void shouldReturnConfiguredProduceQuota() {
        //Given

        //When
        final double actualQuota = staticQuotaSupplier.quotaFor(ClientQuotaType.PRODUCE, Map.of());

        //Then
        assertThat(actualQuota).isEqualTo(PRODUCE_QUOTA, Offset.offset(EPSILON));
    }

    @Test
    void shouldReturnConfiguredFetchQuota() {
        //Given

        //When
        final double actualQuota = staticQuotaSupplier.quotaFor(ClientQuotaType.FETCH, Map.of());

        //Then
        assertThat(actualQuota).isEqualTo(FETCH_QUOTA, Offset.offset(EPSILON));
    }
    @Test
    void shouldReturnConfiguredRequestQuota() {
        //Given

        //When
        final double actualQuota = staticQuotaSupplier.quotaFor(ClientQuotaType.REQUEST, Map.of());

        //Then
        assertThat(actualQuota).isEqualTo(REQUEST_QUOTA, Offset.offset(EPSILON));
    }

    @Test
    void staticQuotaMetrics() {

        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "StaticQuotaCallback");

        assertGaugeMetric(group, "Produce", PRODUCE_QUOTA);
        assertGaugeMetric(group, "Fetch", FETCH_QUOTA);
        assertGaugeMetric(group, "Request", REQUEST_QUOTA);

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
