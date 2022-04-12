/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.strimzi.kafka.quotas.types.Limit;
import io.strimzi.kafka.quotas.types.Volume;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemDataSourceTask implements DataSourceTask {

    private final Limit softLimit;
    private final Limit hardLimit;
    private final long period;

    private final TimeUnit periodUnit = TimeUnit.SECONDS; //TODO Should this also be in config?
    private final Set<FileStore> fileStores;
    private final String brokerId;
    private final Consumer<VolumeUsageMetrics> volumeUsageMetricsConsumer;

    private final Logger log = LoggerFactory.getLogger(FileSystemDataSourceTask.class);

    public FileSystemDataSourceTask(List<Path> logDirs, Limit softLimit, Limit hardLimit, long period, String brokerId, Consumer<VolumeUsageMetrics> volumeUsageMetricsConsumer) {
        this.softLimit = softLimit;
        this.hardLimit = hardLimit;
        this.period = period;
        fileStores = logDirs.stream()
                .filter(Files::exists)
                .map(path -> {
                    try {
                        return Files.getFileStore(path);
                    } catch (IOException e) {
                        log.warn("Unable to get fileStore for {} due to {}", path, e.getMessage(), e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableSet());
        this.brokerId = brokerId;
        this.volumeUsageMetricsConsumer = volumeUsageMetricsConsumer;
    }

    @Override
    public long getPeriod() {
        return period;
    }

    @Override
    public TimeUnit getPeriodUnit() {
        return periodUnit;
    }

    @Override
    public void run() {
        final Instant snapshotAt = Instant.now();
        final List<Volume> volumes = fileStores.stream()
                .map(fs -> {
                    try {
                        return new Volume(fs.name(), fs.getTotalSpace(), fs.getTotalSpace() - fs.getUsableSpace());
                    } catch (IOException e) {
                        log.warn("Unable to read disk usage for {} due to {}", fs.name(), e.getMessage(), e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toUnmodifiableList());
        volumeUsageMetricsConsumer.accept(new VolumeUsageMetrics(brokerId, snapshotAt, hardLimit, softLimit, volumes));
    }
}
