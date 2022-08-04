/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.strimzi.kafka.quotas.distributed.KafkaClientManager;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EnsureRackAwarePartitionAssignmentRunnableTest {
    private static final String TEST_TOPIC = "topic";
    private static final int MY_BROKER_ID = 0;
    private static final Node BROKER_0_RACK_A_NODE = new Node(0, "broker0", 9096, "rackA");
    private static final Node BROKER_1_RACK_B_NODE = new Node(1, "broker1", 9096, "rackB");
    private static final Node BROKER_2_RACK_C_NODE = new Node(2, "broker2", 9096, "rackC");
    private static final Node BROKER_3_RACK_A_NODE = new Node(3, "broker3", 9096, "rackA");
    private static final Node BROKER_4_RACK_B_NODE = new Node(4, "broker4", 9096, "rackB");
    private static final Node BROKER_5_RACK_C_NODE = new Node(5, "broker5", 9096, "rackC");
    private static final Integer REPLICA_COUNT = 3;

    private List<String> activeBrokers = List.of("0", "1", "2");
    private List<Node> activeNodes = List.of(BROKER_0_RACK_A_NODE, BROKER_1_RACK_B_NODE, BROKER_2_RACK_C_NODE, BROKER_3_RACK_A_NODE, BROKER_4_RACK_B_NODE, BROKER_5_RACK_C_NODE);
    private final Supplier<Collection<String>> activeBrokerSupplier = () -> activeBrokers;
    private final Supplier<Collection<Node>> activeNodesSupplier = () -> activeNodes;

    private EnsureRackAwarePartitionAssignmentRunnable ensureRackAwarePartitionAssignmentRunnable;

    @Mock(lenient = true)
    private KafkaClientManager kafkaClientManager;

    @Mock(lenient = true)
    private Admin adminClient;

    @Captor
    private ArgumentCaptor<Collection<String>> describeTopicsCaptor;

    @Mock(lenient = true)
    private DescribeTopicsResult describeTopicsResult;

    @Mock(lenient = true)
    private AlterPartitionReassignmentsResult alterPartitionReassignmentsResult;

    @Captor
    private ArgumentCaptor<Map<TopicPartition, Optional<NewPartitionReassignment>>> reassignmentCaptor;

    @BeforeEach
    void setUp() {
        when(kafkaClientManager.adminClient()).thenReturn(adminClient);

        ensureRackAwarePartitionAssignmentRunnable = new EnsureRackAwarePartitionAssignmentRunnable(kafkaClientManager, activeBrokerSupplier, TEST_TOPIC, REPLICA_COUNT, String.valueOf(MY_BROKER_ID), activeNodesSupplier);
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        when(adminClient.alterPartitionReassignments(anyMap())).thenReturn(alterPartitionReassignmentsResult);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldDiscoverActiveBrokers() {
        //Given
        allowDescribingResilientPartitionLayout();
        final Supplier<Collection<String>> mockActiveBrokerSupplier = mock(Supplier.class);
        when(mockActiveBrokerSupplier.get()).thenReturn(Collections.emptySet());
        ensureRackAwarePartitionAssignmentRunnable = new EnsureRackAwarePartitionAssignmentRunnable(kafkaClientManager, mockActiveBrokerSupplier, TEST_TOPIC, REPLICA_COUNT, String.valueOf(MY_BROKER_ID), activeNodesSupplier);

        //When
        ensureRackAwarePartitionAssignmentRunnable.ensureRackAwarePartitionAssignment(TEST_TOPIC, REPLICA_COUNT);

        //Then
        verify(mockActiveBrokerSupplier).get();
    }

    @Test
    void shouldDescribeConfiguredTopic() {
        //Given
        allowDescribingResilientPartitionLayout();

        //When
        ensureRackAwarePartitionAssignmentRunnable.ensureRackAwarePartitionAssignment(TEST_TOPIC, REPLICA_COUNT);

        //Then
        verify(adminClient).describeTopics(describeTopicsCaptor.capture());
        assertThat(describeTopicsCaptor.getValue()).containsExactly(TEST_TOPIC);
    }

    @Test
    void shouldDoNothingWhenExecutingBrokerHasHigherId() {
        //Given
        activeBrokers = List.of("0", "1", "2");
        ensureRackAwarePartitionAssignmentRunnable = new EnsureRackAwarePartitionAssignmentRunnable(kafkaClientManager, activeBrokerSupplier, TEST_TOPIC, REPLICA_COUNT, "1", activeNodesSupplier);

        //When
        final CompletableFuture<Void> partitionAssignmentFuture = ensureRackAwarePartitionAssignmentRunnable.ensureRackAwarePartitionAssignment(TEST_TOPIC, REPLICA_COUNT);

        //Then
        //Broker 0 *is active* but the instance under test is running as Broker 1
        verify(adminClient, never()).describeTopics(anyCollection());
        assertThat(partitionAssignmentFuture.isDone()).isTrue();
        assertThat(partitionAssignmentFuture.isCompletedExceptionally()).isFalse();
    }

    @Test
    void shouldDescribeTopicWhenExecutedByBrokerWithLowestActiveId() {
        //Given
        allowDescribingResilientPartitionLayout();
        activeBrokers = List.of("1", "2");
        ensureRackAwarePartitionAssignmentRunnable = new EnsureRackAwarePartitionAssignmentRunnable(kafkaClientManager, activeBrokerSupplier, TEST_TOPIC, REPLICA_COUNT, "1", activeNodesSupplier);

        //When
        final CompletableFuture<Void> partitionAssignmentFuture = ensureRackAwarePartitionAssignmentRunnable.ensureRackAwarePartitionAssignment(TEST_TOPIC, REPLICA_COUNT);

        //Then
        //Broker 0 *is not active* and this is being executed by broker 1
        verify(adminClient).describeTopics(anyCollection());
        assertThat(partitionAssignmentFuture.isDone()).isTrue();
        assertThat(partitionAssignmentFuture.isCompletedExceptionally()).isFalse();
    }

    @Test
    void shouldAlterPartitionAssignment() {
        //Given
        allowDescribingInvalidPartitionLayout();
        final TopicPartition expectedTopicPartition = new TopicPartition(TEST_TOPIC, 0);
        when(alterPartitionReassignmentsResult.all()).thenReturn(KafkaFuture.completedFuture(null));

        //When
        ensureRackAwarePartitionAssignmentRunnable.ensureRackAwarePartitionAssignment(TEST_TOPIC, REPLICA_COUNT);

        //Then
        verify(adminClient).alterPartitionReassignments(reassignmentCaptor.capture());
        final Map<TopicPartition, Optional<NewPartitionReassignment>> reassignmentDetails = reassignmentCaptor.getValue();
        assertThat(reassignmentDetails).hasSize(1);
        assertThat(reassignmentDetails).hasKeySatisfying(new Condition<>(Predicate.isEqual(expectedTopicPartition), TEST_TOPIC + "-0"));
        final Optional<NewPartitionReassignment> newPartitionReassignment = reassignmentDetails.get(expectedTopicPartition);
        assertThat(newPartitionReassignment).isPresent();
        assertThat(newPartitionReassignment.get().targetReplicas()).describedAs("partition Target replicas").hasSize(REPLICA_COUNT);
    }

    @Test
    void shouldNotAlterPartitionAssignmentWhenDistributedAcrossRacks() {
        //Given
        allowDescribingResilientPartitionLayout();

        //When
        final CompletableFuture<Void> partitionAssignmentFuture = ensureRackAwarePartitionAssignmentRunnable.ensureRackAwarePartitionAssignment(TEST_TOPIC, REPLICA_COUNT);

        //Then
        verify(adminClient, never()).alterPartitionReassignments(anyMap());
        assertThat(partitionAssignmentFuture.isDone()).isTrue();
        assertThat(partitionAssignmentFuture.isCompletedExceptionally())
                .describedAs("future should be completed successfully")
                .isFalse();
    }

    @Test
    void shouldNotAlterPartitionAssignmentWhenThereAreNotEnoughRacks() {
        //Given
        activeNodes = List.of(BROKER_1_RACK_B_NODE, BROKER_4_RACK_B_NODE, BROKER_2_RACK_C_NODE, BROKER_5_RACK_C_NODE);
        allowDescribingInvalidPartitionLayout();

        //When
        final CompletableFuture<Void> partitionAssignmentFuture = ensureRackAwarePartitionAssignmentRunnable.ensureRackAwarePartitionAssignment(TEST_TOPIC, REPLICA_COUNT);

        //Then
        verify(adminClient, never()).alterPartitionReassignments(anyMap());
        assertThat(partitionAssignmentFuture.isDone()).isTrue();
        assertThat(partitionAssignmentFuture.isCompletedExceptionally())
                .describedAs("future should be completed successfully")
                .isFalse();
    }

    @Test
    void shouldFailFutureIfPartitionAssignmentThrows() {
        //Given
        allowDescribingInvalidPartitionLayout();
        final KafkaFutureImpl<Void> failingFuture = new KafkaFutureImpl<>();
        failingFuture.completeExceptionally(new UnknownTopicOrPartitionException("Testing :D"));
        when(alterPartitionReassignmentsResult.all()).thenReturn(failingFuture);

        //When
        final CompletableFuture<Void> future = ensureRackAwarePartitionAssignmentRunnable.ensureRackAwarePartitionAssignment(TEST_TOPIC, REPLICA_COUNT);

        //Then
        assertThat(future.isDone()).isTrue();
        assertThat(future.isCompletedExceptionally()).isTrue();

    }

    private void allowDescribingInvalidPartitionLayout() {
        allowDescribingPartitionLayout(BROKER_0_RACK_A_NODE, BROKER_2_RACK_C_NODE, BROKER_3_RACK_A_NODE);
    }

    private void allowDescribingResilientPartitionLayout() {
        allowDescribingPartitionLayout(BROKER_0_RACK_A_NODE, BROKER_1_RACK_B_NODE, BROKER_2_RACK_C_NODE);
    }

    private void allowDescribingPartitionLayout(Node... nodes) {
        when(describeTopicsResult.all()).thenReturn(
                KafkaFuture.completedFuture(Map.of(TEST_TOPIC, new TopicDescription(TEST_TOPIC, false, List.of(
                        new TopicPartitionInfo(0,
                                BROKER_0_RACK_A_NODE,
                                List.of(nodes),
                                List.of(nodes))
                )))));
    }
}
