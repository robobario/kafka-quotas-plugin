/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.strimzi.kafka.quotas.distributed.KafkaClientManager;
import io.strimzi.kafka.quotas.local.UnlimitedQuotaSupplier;
import io.strimzi.kafka.quotas.types.VolumeUsageMetrics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.strimzi.kafka.quotas.StaticQuotaConfig.QUOTA_POLICY_INTERVAL_PROP;
import static io.strimzi.kafka.quotas.StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StaticQuotaCallbackTest {

    private static final String TEST_TOPIC = "wibble";

    StaticQuotaCallback target;
    @Mock(lenient = true)
    private KafkaClientManager kafkaClientManager;

    @Captor
    private ArgumentCaptor<Collection<NewTopic>> newTopicsCaptor;

    @BeforeEach
    void setup() {
        target = new StaticQuotaCallback(new StorageChecker(), Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);
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
    void pluginLifecycle() throws Exception {
        StorageChecker storageChecker = mock(StorageChecker.class);
        StaticQuotaCallback target = new StaticQuotaCallback(storageChecker, Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);
        target.configure(Map.of());
        target.updateClusterMetadata(null);
        verify(storageChecker, times(1)).startIfNecessary();
        target.close();
        verify(storageChecker, times(1)).stop();
    }

    @Test
    void quotaResetRequired() {
        StorageChecker mock = mock(StorageChecker.class);
        ArgumentCaptor<Consumer<Long>> argument = ArgumentCaptor.forClass(Consumer.class);
        doNothing().when(mock).configure(anyLong(), anyList(), argument.capture());
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(mock, Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);
        quotaCallback.configure(Map.of());
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

    @Test
    void shouldNotScheduleDataSourceTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), executorService, this::spyOnQuotaConfig, kafkaClientManager);

        //When
        staticQuotaCallback.configure(Map.of());

        //Then
        verifyNoInteractions(executorService);
    }

    @Test
    void shouldScheduleDataSourceTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), executorService, this::spyOnQuotaConfig, kafkaClientManager);
        final Long interval = 10L;

        //When
        staticQuotaCallback.configure(Map.of(STORAGE_CHECK_INTERVAL_PROP, interval.intValue()));

        //Then
        verify(executorService).scheduleWithFixedDelay(isA(DataSourceTask.class), anyLong(), eq(interval), eq(TimeUnit.SECONDS));
    }

    @Test
    void shouldCancelExistingScheduleDataSourceTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), executorService, this::spyOnQuotaConfig, kafkaClientManager);
        final ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        doReturn(scheduledFuture).when(executorService).scheduleWithFixedDelay(isA(DataSourceTask.class), anyLong(), anyLong(), any());
        final int interval = 10;
        staticQuotaCallback.configure(Map.of(STORAGE_CHECK_INTERVAL_PROP, interval));

        //When
        staticQuotaCallback.configure(Map.of(STORAGE_CHECK_INTERVAL_PROP, interval));

        //Then
        verify(scheduledFuture).cancel(false);
    }

    @Test
    void shouldNotScheduleQuotaPolicyTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), executorService, this::spyOnQuotaConfig, kafkaClientManager);

        //When
        staticQuotaCallback.configure(Map.of(QUOTA_POLICY_INTERVAL_PROP, 0));

        //Then
        verifyNoInteractions(executorService);
    }

    @Test
    void shouldScheduleQuotaPolicyTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), executorService, this::spyOnQuotaConfig, kafkaClientManager);
        final Long interval = 10L;

        //When
        staticQuotaCallback.configure(Map.of(QUOTA_POLICY_INTERVAL_PROP, interval.intValue()));

        //Then
        verify(executorService).scheduleWithFixedDelay(isA(QuotaPolicyTask.class), anyLong(), eq(interval), eq(TimeUnit.SECONDS));
    }

    @Test
    void shouldCancelExistingScheduleQuotaPolicyTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), executorService, this::spyOnQuotaConfig, kafkaClientManager);
        final ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        doReturn(scheduledFuture).when(executorService).scheduleWithFixedDelay(isA(QuotaPolicyTask.class), anyLong(), anyLong(), any());
        final int interval = 10;
        staticQuotaCallback.configure(Map.of(QUOTA_POLICY_INTERVAL_PROP, interval));

        //When
        staticQuotaCallback.configure(Map.of(QUOTA_POLICY_INTERVAL_PROP, interval));

        //Then
        verify(scheduledFuture).cancel(false);
    }

    @Test
    void shouldShutdownExecutorServiceOnClose() {

        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), executorService, this::spyOnQuotaConfig, kafkaClientManager);
        when(executorService.shutdownNow()).thenReturn(List.of());

        //When
        staticQuotaCallback.close();

        //Then
        verify(executorService).shutdownNow();
    }

    @Test
    void shouldPropagateCloseToKafkaClientManager() throws IOException {
        //Given
        final KafkaClientManager kafkaClientManager = mock(KafkaClientManager.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);

        //When
        staticQuotaCallback.close();

        //Then
        verify(kafkaClientManager).close();
    }

    @Test
    void shouldPropagateConfigurationToKafkaClientManager() {
        //Given
        final KafkaClientManager kafkaClientManager = mock(KafkaClientManager.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);
        final Map<String, Object> configMap = Map.of();

        //When
        staticQuotaCallback.configure(configMap);

        //Then
        verify(kafkaClientManager).configure(eq(configMap));
    }

    @Test
    void shouldRequestNewTopic() {
        //Given
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);
        final Admin adminClient = stubDescribeTopicsForMissingTopic();
        stubCreationOfExistingTopic(adminClient);

        //When
        final CompletableFuture<Void> topicFuture = staticQuotaCallback.ensureTopicIsAvailable(TEST_TOPIC, spyOnQuotaConfig(Map.of(), false));

        //Then
        verify(adminClient).createTopics(newTopicsCaptor.capture());
        assertThat(newTopicsCaptor.getValue()).contains(new NewTopic(TEST_TOPIC, Optional.of(3), Optional.empty()));
        assertThat(topicFuture).isCompleted();
    }

    @Test
    void shouldNotRequestNewTopic() {
        //Given
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);
        final Admin adminClient = stubDescribeTopicsForExistingTopic();

        //When
        final CompletableFuture<Void> topicFuture = staticQuotaCallback.ensureTopicIsAvailable(TEST_TOPIC, spyOnQuotaConfig(Map.of(), false));

        //Then
        verify(adminClient, never()).createTopics(anyCollection());
        assertThat(topicFuture).isCompleted();
    }

    @Test
    void shouldSurviveTopicCreationRace() {
        //Given
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(new StorageChecker(), Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);
        final Admin adminClient = stubDescribeTopicsForMissingTopic();
        stubCreationOfExistingTopic(adminClient);

        //When
        final CompletableFuture<Void> topicFuture = staticQuotaCallback.ensureTopicIsAvailable(TEST_TOPIC, spyOnQuotaConfig(Map.of(), false));

        //Then
        verify(adminClient).createTopics(newTopicsCaptor.capture());
        assertThat(newTopicsCaptor.getValue()).contains(new NewTopic(TEST_TOPIC, Optional.of(3), Optional.empty()));
        assertThat(topicFuture).isCompleted();
    }

    private void stubCreationOfExistingTopic(Admin adminClient) {
        final KafkaFutureImpl<Void> createTopicFuture = new KafkaFutureImpl<>();
        createTopicFuture.completeExceptionally(new ExecutionException(new TopicExistsException("haha beat you to it...")));
        final CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);

        when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(createTopicFuture);
    }

    private Admin stubDescribeTopicsForMissingTopic() {
        final KafkaFutureImpl<Map<String, TopicDescription>> kafkaFuture = new KafkaFutureImpl<>();
        kafkaFuture.completeExceptionally(new ExecutionException(new UnknownTopicOrPartitionException()));

        return stubDescribeTopics(kafkaFuture);
    }

    private Admin stubDescribeTopicsForExistingTopic() {
        final KafkaFutureImpl<Map<String, TopicDescription>> kafkaFuture = new KafkaFutureImpl<>();
        kafkaFuture.complete(Map.of(TEST_TOPIC, new TopicDescription(TEST_TOPIC, false, List.of())));

        return stubDescribeTopics(kafkaFuture);
    }

    private Admin stubDescribeTopics(KafkaFuture<Map<String, TopicDescription>> kafkaFuture) {
        final Admin adminClient = mock(Admin.class);
        when(kafkaClientManager.adminClient()).thenReturn(adminClient);
        final DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.all()).thenReturn(kafkaFuture);
        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        return adminClient;
    }

    //TODO this is still a code smell.
    private StaticQuotaConfig spyOnQuotaConfig(Map<String, ?> config, Boolean doLog) {
        StaticQuotaConfig staticQuotaConfig = spy(new StaticQuotaConfig(config, doLog));
        staticQuotaConfig.withKafkaClientManager(kafkaClientManager);
        lenient().doReturn(UnlimitedQuotaSupplier.UNLIMITED_QUOTA_SUPPLIER).when(staticQuotaConfig).quotaFactorSupplier();
        lenient().doReturn((Supplier<Iterable<VolumeUsageMetrics>>) List::of).when(staticQuotaConfig).volumeUsageMetricsSupplier();
        lenient().doReturn((Consumer<VolumeUsageMetrics>) volumeUsageMetrics -> {
        }).when(staticQuotaConfig).volumeUsageMetricsPublisher();
        return staticQuotaConfig;
    }
}
