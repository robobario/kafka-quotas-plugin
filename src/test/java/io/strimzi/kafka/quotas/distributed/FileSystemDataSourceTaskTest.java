/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.distributed;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.FileSystemDataSourceTask;
import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.Volume;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSystemDataSourceTaskTest {

    private static final String BROKER_ID = "1";
    private static final long VOLUME_CAPACITY = Configuration.Builder.DEFAULT_MAX_SIZE;
    private static final long GIBI_BYTE_MULTIPLIER = 1024 * 1024 * 1024;
    private static final long CONSUMED_BYTES_HARD_LIMIT = VOLUME_CAPACITY - GIBI_BYTE_MULTIPLIER;
    private static final long CONSUMED_BYTES_SOFT_LIMIT = VOLUME_CAPACITY - (2 * GIBI_BYTE_MULTIPLIER);
    private static final int BLOCK_SIZE = Configuration.Builder.DEFAULT_BLOCK_SIZE;
    private static final String FILE_CONTENT = "sdfjdsaklfasdlkflkdsjfklasdjflksjfkljadsfkljasdl";
    private final List<VolumeUsageMetrics> actualMetricUpdates = new ArrayList<>();

    private FileSystem mockFileStore;
    private FileSystemDataSourceTask dataSourceTask;
    private Path logDir;
    private Limit consumedBytesHardLimit;
    private Limit consumedBytesSoftLimit;

    @BeforeEach
    void setUp() throws IOException {
        mockFileStore = Jimfs.newFileSystem("MockFileStore", Configuration.unix());
        final Path dataPath = mockFileStore.getPath(mockFileStore.getSeparator());
        logDir = Files.createDirectories(dataPath);

        consumedBytesHardLimit = new Limit(Limit.LimitType.CONSUMED_BYTES, CONSUMED_BYTES_HARD_LIMIT);
        consumedBytesSoftLimit = new Limit(Limit.LimitType.CONSUMED_BYTES, CONSUMED_BYTES_SOFT_LIMIT);
        dataSourceTask = new FileSystemDataSourceTask(List.of(logDir), consumedBytesSoftLimit, consumedBytesHardLimit, 10, BROKER_ID, actualMetricUpdates::add);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (mockFileStore != null) {
            mockFileStore.close();
        }
        Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> "io.strimzi.kafka.quotas.StaticQuotaCallback".equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
    }

    @Test
    void shouldPublishVolumeMetrics() throws IOException {
        //Given
        final long usedBytes = prepareFileStore(logDir, FILE_CONTENT);

        //When
        dataSourceTask.run();

        //Then
        assertThat(actualMetricUpdates).hasSize(1);
        VolumeUsageMetrics actualMetrics = actualMetricUpdates.get(0);
        assertThat(actualMetrics).isNotNull();

        final Volume expectedVolume = new Volume(Files.getFileStore(logDir).name(), VOLUME_CAPACITY, usedBytes);
        assertThat(actualMetrics.getVolumes()).contains(expectedVolume);
    }

    @Test
    void shouldPublishesChanges() throws IOException {
        //Given
        long usedBytes = prepareFileStore(logDir, FILE_CONTENT);
        dataSourceTask.run();
        usedBytes += prepareFileStore(logDir, FILE_CONTENT.repeat(1001));

        //When
        dataSourceTask.run();

        //Then
        assertThat(actualMetricUpdates).hasSize(2);
        VolumeUsageMetrics actualMetrics = actualMetricUpdates.get(1);
        assertThat(actualMetrics).isNotNull();

        final Volume expectedVolume = new Volume(Files.getFileStore(logDir).name(), VOLUME_CAPACITY, usedBytes);
        assertThat(actualMetrics.getVolumes()).contains(expectedVolume);
    }

    @Test
    void shouldIncludeHardLimitInSnapshot() {
        //Given

        //When
        dataSourceTask.run();

        //Then
        assertThat(actualMetricUpdates).hasSize(1);
        VolumeUsageMetrics actualMetrics = actualMetricUpdates.get(0);
        assertThat(actualMetrics.getHardLimit()).isNotNull().isEqualTo(consumedBytesHardLimit);
    }

    @Test
    void shouldIncludeSoftLimitInSnapshot() {
        //Given

        //When
        dataSourceTask.run();

        //Then
        assertThat(actualMetricUpdates).hasSize(1);
        VolumeUsageMetrics actualMetrics = actualMetricUpdates.get(0);
        assertThat(actualMetrics.getSoftLimit()).isNotNull().isEqualTo(consumedBytesSoftLimit);
    }

    @Test
    void storageCheckerMetrics() throws IOException {
        //Given
        final long usedBytes = prepareFileStore(logDir, FILE_CONTENT);

        //When
        dataSourceTask.run();

        //Then
        assertThat(actualMetricUpdates).hasSize(1);
        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "StorageChecker");

        assertGaugeMetric(group, "SoftLimitBytes", CONSUMED_BYTES_SOFT_LIMIT);
        assertGaugeMetric(group, "HardLimitBytes", CONSUMED_BYTES_HARD_LIMIT);
        assertGaugeMetric(group, "TotalStorageUsedBytes", usedBytes);

        // the mbean name is part of the public api
        MetricName name = group.firstKey();
        String expectedMbeanName = String.format("io.strimzi.kafka.quotas:type=StorageChecker,name=%s", name.getName());
        assertEquals(expectedMbeanName, name.getMBeanName(), "unexpected mbean name");
    }

    private long prepareFileStore(Path fileStorePath, String fileContent) throws IOException {
        Path file = Files.createTempFile(fileStorePath, "t", ".tmp");
        Files.writeString(file, fileContent);
        final long fileSize = Files.size(file);
        long numBlocks;
        if (fileSize <= BLOCK_SIZE) {
            numBlocks = 1;
        } else if (fileSize % BLOCK_SIZE == 0) {
            numBlocks = fileSize / BLOCK_SIZE;
        } else {
            numBlocks = (fileSize / BLOCK_SIZE) + 1;
        }
        return numBlocks * BLOCK_SIZE;
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
