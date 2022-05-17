/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Optional;
import java.util.SortedMap;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.types.Volume;
import org.assertj.core.data.Offset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUtils {
    public static final double EPSILON = 0.00001;

    public static final Offset<Double> EPSILON_OFFSET = Offset.offset(TestUtils.EPSILON);

    public static Volume newVolumeWith(long consumedCapacity) {
        return new Volume("Disk One", 20L, consumedCapacity);
    }

    public static SortedMap<MetricName, Metric> getMetricGroup(String p, String t) {
        SortedMap<String, SortedMap<MetricName, Metric>> storageMetrics = Metrics.defaultRegistry().groupedMetrics((name, metric) -> p.equals(name.getScope()) && t.equals(name.getType()));
        assertEquals(1, storageMetrics.size(), "unexpected number of metrics in group");
        return storageMetrics.entrySet().iterator().next().getValue();
    }

    public static <T> void assertGaugeMetric(SortedMap<MetricName, Metric> metrics, String name, T expected) {
        Optional<Gauge<T>> desired = findGaugeMetric(metrics, name);
        assertTrue(desired.isPresent(), String.format("metric with name %s not found in %s", name, metrics));
        Gauge<T> gauge = desired.get();
        assertEquals(expected, gauge.value(), String.format("metric %s has unexpected value", name));
    }

    @SuppressWarnings("unchecked")
    private static <T> Optional<Gauge<T>> findGaugeMetric(SortedMap<MetricName, Metric> metrics, String name) {
        return metrics.entrySet().stream().filter(e -> name.equals(e.getKey().getName())).map(e -> (Gauge<T>) e.getValue()).findFirst();
    }

    public static void assertCounterMetric(SortedMap<MetricName, Metric> metrics, String name, long expected) {
        Optional<Counter> desired = findCounterMetric(metrics, name);
        assertTrue(desired.isPresent(), String.format("metric with name %s not found in %s", name, metrics));
        Counter counter = desired.get();
        assertEquals(expected, counter.count(), String.format("metric %s has unexpected value", name));
    }

    private static Optional<Counter> findCounterMetric(SortedMap<MetricName, Metric> metrics, String name) {
        return metrics.entrySet().stream().filter(e -> name.equals(e.getKey().getName())).map(e -> (Counter) e.getValue()).findFirst();
    }
}
