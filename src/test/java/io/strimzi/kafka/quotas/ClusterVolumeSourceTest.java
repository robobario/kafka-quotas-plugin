/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterVolumeSourceTest {

    private VolumeConsumer volumeConsumer;
    private ClusterVolumeSource clusterVolumeSource;
    @Mock
    private Admin admin;

    private final Map<Integer, Map<String, LogDirDescription>> descriptions = new HashMap<>();

    private final List<Node> nodes = Lists.newArrayList();

    @BeforeEach
    void setUp() {
        volumeConsumer = new VolumeConsumer();
        clusterVolumeSource = new ClusterVolumeSource(admin, volumeConsumer);
        final DescribeClusterResult mockDescribeClusterResult = Mockito.mock(DescribeClusterResult.class);
        when(mockDescribeClusterResult.nodes()).thenReturn(KafkaFuture.completedFuture(nodes));
        when(admin.describeCluster()).thenReturn(mockDescribeClusterResult);
        DescribeLogDirsResult mockDescribeLogDirsResult = Mockito.mock(DescribeLogDirsResult.class);
        when(mockDescribeLogDirsResult.allDescriptions()).thenReturn(KafkaFuture.completedFuture(descriptions));
        when(admin.describeLogDirs(argThat(integers ->
                integers.equals(nodes.stream().map(Node::id).collect(Collectors.toSet())))))
                .thenReturn(mockDescribeLogDirsResult);
    }

    @Test
    void shouldProduceCollectionOfVolumes() {
        //Given
        final int nodeId = 1;
        givenNode(nodeId);
        givenLogDirDescription(nodeId, "dir1", 50, 10);

        //When
        clusterVolumeSource.run();

        //Then
        final List<Collection<Volume>> results = volumeConsumer.getActualResults();
        assertThat(results).hasSize(1);
        final Collection<Volume> onlyInvocation = results.get(0);
        assertThat(onlyInvocation).containsExactly(new Volume("1", "dir1", 50, 10));
    }

    @Test
    void shouldProduceMultipleVolumesForASingleBrokerIfItHasMultipleLogDirs() {
        //Given
        final int nodeId = 1;
        givenNode(nodeId);
        givenLogDirDescription(nodeId, "dir1", 50, 10);
        givenLogDirDescription(nodeId, "dir2", 30, 5);

        //When
        clusterVolumeSource.run();

        //Then
        final List<Collection<Volume>> results = volumeConsumer.getActualResults();
        assertThat(results).hasSize(1);
        final Collection<Volume> onlyInvocation = results.get(0);
        Volume expected1 = new Volume("1", "dir1", 50, 10);
        Volume expected2 = new Volume("1", "dir2", 30, 5);
        assertThat(onlyInvocation).containsExactlyInAnyOrder(expected1, expected2);
    }

    @Test
    void shouldProduceMultipleVolumesForMultipleBrokers() {
        //Given
        final int nodeId = 1;
        givenNode(nodeId);
        givenLogDirDescription(nodeId, "dir1", 50, 10);

        int nodeId2 = 2;
        givenNode(nodeId2);
        givenLogDirDescription(nodeId2, "dir1", 30, 5);

        //When
        clusterVolumeSource.run();

        //Then
        final List<Collection<Volume>> results = volumeConsumer.getActualResults();
        assertThat(results).hasSize(1);
        final Collection<Volume> onlyInvocation = results.get(0);
        Volume expected1 = new Volume("1", "dir1", 50, 10);
        Volume expected2 = new Volume("2", "dir1", 30, 5);
        assertThat(onlyInvocation).containsExactlyInAnyOrder(expected1, expected2);
    }

    @Test
    void shouldProduceEmptyIfDescribeLogDirsReturnsEmptyMaps() {
        //Given
        givenNode(1);

        //When
        clusterVolumeSource.run();

        //Then
        final List<Collection<Volume>> results = volumeConsumer.getActualResults();
        assertThat(results).hasSize(1);
        final Collection<Volume> onlyInvocation = results.get(0);
        assertThat(onlyInvocation).isEmpty();
    }

    private static class VolumeConsumer implements Consumer<Collection<Volume>> {
        private final List<Collection<Volume>> actualResults = new ArrayList<>();

        @Override
        public void accept(Collection<Volume> volumes) {
            actualResults.add(volumes);
        }

        public List<Collection<Volume>> getActualResults() {
            return actualResults;
        }
    }


    private void givenNode(int nodeId) {
        final Node singleNode = new Node(nodeId, "broker1", 9092);
        nodes.add(singleNode);
    }

    private void givenLogDirDescription(int nodeId, String logDir, int totalBytes, int usableBytes) {
        final LogDirDescription dirDescription = new LogDirDescription(null, Map.of(), totalBytes, usableBytes);
        descriptions.computeIfAbsent(nodeId, HashMap::new).put(logDir, dirDescription);
    }

}
