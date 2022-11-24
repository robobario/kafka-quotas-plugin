/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toSet;

public class ClusterVolumeSource implements Runnable {

    private final Consumer<Collection<Volume>> volumeConsumer;
    private final Admin admin;

    private static final Logger log = LoggerFactory.getLogger(ClusterVolumeSource.class);

    public ClusterVolumeSource(Admin admin, Consumer<Collection<Volume>> volumeConsumer) {
        this.volumeConsumer = volumeConsumer;
        this.admin = admin;
    }

    @Override
    public void run() {
        log.debug("Attempting to describe cluster");
        final DescribeClusterResult clusterResult = admin.describeCluster();
        clusterResult.nodes().whenComplete((nodes, throwable) -> {
            if (throwable != null) {
                log.error("error while describing cluster", throwable);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Successfully described cluster: " + nodes);
                }
                onClusterDescribeSuccess(nodes);
            }
        });
    }

    private void onClusterDescribeSuccess(Collection<Node> nodes) {
        final Set<Integer> allBrokerIds = nodes.stream().map(Node::id).collect(toSet());
        final DescribeLogDirsResult logDirsResult = admin.describeLogDirs(allBrokerIds);
        logDirsResult.allDescriptions().whenComplete((logDirsPerBroker, throwable) -> {
            if (throwable != null) {
                log.error("error while describing log dirs", throwable);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Successfully described logDirs: " + logDirsPerBroker);
                }
                onDescribeLogDirsSuccess(logDirsPerBroker);
            }
        });
    }

    private void onDescribeLogDirsSuccess(Map<Integer, Map<String, LogDirDescription>> logDirsPerBroker) {
        final List<Volume> volumes = logDirsPerBroker.entrySet().stream()
                .flatMap(ClusterVolumeSource::toVolumes).collect(Collectors.toUnmodifiableList());
        if (log.isDebugEnabled()) {
            log.debug("Notifying consumers of volumes: " + volumes);
        }
        volumeConsumer.accept(volumes);
    }


    private static Stream<? extends Volume> toVolumes(Map.Entry<Integer, Map<String, LogDirDescription>> brokerIdToLogDirs) {
        return brokerIdToLogDirs.getValue().entrySet().stream().map(logDirs -> {
            LogDirDescription logDirDescription = logDirs.getValue();
            final long totalBytes = logDirDescription.totalBytes().orElse(0);
            final long usableBytes = logDirDescription.usableBytes().orElse(0);
            return new Volume("" + brokerIdToLogDirs.getKey(), logDirs.getKey(), totalBytes, usableBytes);
        });
    }

}
