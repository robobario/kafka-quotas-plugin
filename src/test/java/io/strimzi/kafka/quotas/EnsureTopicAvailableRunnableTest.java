/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.strimzi.kafka.quotas.distributed.KafkaClientManager;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EnsureTopicAvailableRunnableTest {
    private static final String TEST_TOPIC = "topic";

    @Mock
    KafkaClientManager kafkaClientManager;
    @Mock(lenient = true)
    private Admin adminClient;

    @Captor
    private ArgumentCaptor<Collection<NewTopic>> newTopicsCaptor;

    private EnsureTopicAvailableRunnable ensureTopicAvailableRunnable;

    @BeforeEach
    void setUp() {
        when(kafkaClientManager.adminClient()).thenReturn(adminClient);
        ensureTopicAvailableRunnable = new EnsureTopicAvailableRunnable(kafkaClientManager, TEST_TOPIC, 3);
    }

    @Test
    void shouldRequestNewTopic() {
        //Given
        final NewTopic expectedNewTopic = new NewTopic(TEST_TOPIC, Optional.of(1), Optional.empty());
        expectedNewTopic.configs(Map.of("cleanup.policy", "compact"));
        stubCreationOfMissingTopic();

        //When
        final CompletableFuture<Void> topicFuture = ensureTopicAvailableRunnable.ensureTopicIsAvailable(TEST_TOPIC, 1);

        //Then
        verify(adminClient).createTopics(newTopicsCaptor.capture());
        assertThat(newTopicsCaptor.getValue()).containsExactly(expectedNewTopic);
        assertThat(topicFuture).isCompleted();
    }

    @Test
    void shouldContinueIfTopicExistsAlready() {
        //Given
        stubCreationOfExistingTopic();

        //When
        final CompletableFuture<Void> topicFuture = ensureTopicAvailableRunnable.ensureTopicIsAvailable(TEST_TOPIC, 3);

        //Then
        verify(adminClient).createTopics(anyCollection());
        assertThat(topicFuture).isCompleted();
    }

    private void stubCreationOfExistingTopic() {
        final KafkaFutureImpl<Void> createTopicFuture = new KafkaFutureImpl<>();
        createTopicFuture.completeExceptionally(new ExecutionException(new TopicExistsException("haha beat you to it...")));
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);

        when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(createTopicFuture);
    }

    private void stubCreationOfMissingTopic() {
        final KafkaFutureImpl<Void> createTopicFuture = new KafkaFutureImpl<>();
        createTopicFuture.complete(null);
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);

        when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(createTopicFuture);
    }

}
