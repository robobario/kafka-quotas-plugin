/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.List;
import java.util.Map;
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
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.strimzi.kafka.quotas.StaticQuotaConfig.QUOTA_POLICY_INTERVAL_PROP;
import static io.strimzi.kafka.quotas.StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP;
import static io.strimzi.kafka.quotas.TestUtils.EPSILON_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StaticQuotaCallbackTest {

    private StaticQuotaCallback target;

    @Mock(lenient = true)
    private Admin adminClient;

    @Mock(lenient = true)
    private KafkaClientManager kafkaClientManager;

    @BeforeEach
    void setup() {
        when(kafkaClientManager.adminClient()).thenReturn(adminClient);

        target = new StaticQuotaCallback(Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);
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
    void shouldApplyQuotaFactor() {
        //Given
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target = new StaticQuotaCallback(Executors.newSingleThreadScheduledExecutor(),
            (stringMap, aBoolean) -> spyOnQuotaConfig(stringMap, aBoolean, new FixedQuotaFactorSupplier(0.5D)),
            kafkaClientManager);
        target.configure(Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024));

        //When
        double quotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));

        //Then
        assertThat(quotaLimit).isEqualTo(512.0, EPSILON_OFFSET);
    }

    @Test
    void shouldPauseForZeroQuotaFactor() {
        //Given
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target = new StaticQuotaCallback(Executors.newSingleThreadScheduledExecutor(),
            (stringMap, aBoolean) -> spyOnQuotaConfig(stringMap, aBoolean, new FixedQuotaFactorSupplier(0.0)),
            kafkaClientManager);
        target.configure(Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024));

        //When
        double quotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));

        //Then
        assertThat(quotaLimit).isEqualTo(1.0, EPSILON_OFFSET);
    }

    @Test
    void shouldReturnNonZeroQuota() {
        //Given
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target = new StaticQuotaCallback(Executors.newSingleThreadScheduledExecutor(),
            (stringMap, aBoolean) -> spyOnQuotaConfig(stringMap, aBoolean, new FixedQuotaFactorSupplier(0.0D)),
            kafkaClientManager);
        target.configure(Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024));

        //When
        double quotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));

        //Then
        assertThat(quotaLimit).isEqualTo(1.0, EPSILON_OFFSET);
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
    void shouldMarkSuperUserAsExcludedPrincipal() {
        //Given
        KafkaPrincipal superUser = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "super.user");
        target.configure(Map.of(
                "super.users", "User:super.user",
                StaticQuotaConfig.EXCLUDED_PRINCIPAL_NAME_LIST_PROP, "bar",
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024));

        //When
        final Map<String, String> metricTags = target.quotaMetricTags(ClientQuotaType.PRODUCE, superUser, "clientId");

        //Then
        assertThat(metricTags).containsEntry("excluded-principal-quota-key", "true");
    }

    @ParameterizedTest(name = "quotaResetRequired: {argumentsWithNames}")
    @EnumSource(ClientQuotaType.class)
    void quotaResetRequired(ClientQuotaType quotaType) {
        //Given
        final QuotaFactorSupplier quotaFactorSupplier = mock(QuotaFactorSupplier.class);
        target = new StaticQuotaCallback(Executors.newSingleThreadScheduledExecutor(),
            (stringMap, aBoolean) -> spyOnQuotaConfig(stringMap, aBoolean, quotaFactorSupplier),
            kafkaClientManager);
        final ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        doNothing().when(quotaFactorSupplier).addUpdateListener(captor.capture());
        target.configure(Map.of());
        final Runnable factorUpdateListener = captor.getValue();
        //Validate initial state
        assertTrue(target.quotaResetRequired(quotaType), "unexpected initial state");
        assertFalse(target.quotaResetRequired(quotaType), "unexpected state on subsequent call without storage state change");

        //When
        factorUpdateListener.run();

        //Then
        // (only quota type produce is affected by the storage state change).
        assertEquals(quotaType == ClientQuotaType.PRODUCE, target.quotaResetRequired(quotaType), "unexpected state following storage state change for quota type : " + quotaType);
        assertFalse(target.quotaResetRequired(quotaType), "unexpected state on subsequent call without storage state change");

    }

    @Test
    void shouldNotScheduleDataSourceTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);

        //When
        staticQuotaCallback.configure(Map.of());

        //Then
        verifyNoInteractions(executorService);
    }

    @Test
    void shouldScheduleDataSourceTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);
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
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);
        final ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        doReturn(scheduledFuture).when(executorService).scheduleWithFixedDelay(isA(DataSourceTask.class), anyLong(), anyLong(), any());
        doReturn(null).when(executorService).scheduleWithFixedDelay(isA(EnsureTopicAvailableRunnable.class), anyLong(), anyLong(), any());
        final int interval = 10;
        staticQuotaCallback.configure(Map.of(STORAGE_CHECK_INTERVAL_PROP, interval));

        //When
        //Because calling configure again could potentially re-configure the scheduledTask it should be cancelled and re-scheduled
        staticQuotaCallback.configure(Map.of(STORAGE_CHECK_INTERVAL_PROP, interval));

        //Then
        verify(scheduledFuture).cancel(false);
    }

    @Test
    void shouldNotScheduleEnsureTopicAvailableRunnable() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);

        //When
        staticQuotaCallback.configure(Map.of());

        //Then
        verifyNoInteractions(executorService);
    }

    @Test
    void shouldScheduleEnsureTopicAvailableRunnable() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);
        final Long interval = 10L;

        //When
        staticQuotaCallback.configure(Map.of(STORAGE_CHECK_INTERVAL_PROP, interval.intValue()));

        //Then
        verify(executorService).scheduleWithFixedDelay(isA(EnsureTopicAvailableRunnable.class), anyLong(), eq(interval), eq(TimeUnit.SECONDS));
    }

    @Test
    void shouldCancelExistingScheduleEnsureTopicAvailableRunnable() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);
        final ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        doReturn(null).when(executorService).scheduleWithFixedDelay(isA(DataSourceTask.class), anyLong(), anyLong(), any());
        doReturn(scheduledFuture).when(executorService).scheduleWithFixedDelay(isA(EnsureTopicAvailableRunnable.class), anyLong(), anyLong(), any());
        final int interval = 10;
        staticQuotaCallback.configure(Map.of(STORAGE_CHECK_INTERVAL_PROP, interval));

        //When
        //Because calling configure again could potentially re-configure the scheduledTask it should be cancelled and re-scheduled
        staticQuotaCallback.configure(Map.of(STORAGE_CHECK_INTERVAL_PROP, interval));

        //Then
        verify(scheduledFuture).cancel(false);
    }

    @Test
    void shouldNotScheduleQuotaPolicyTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);

        //When
        staticQuotaCallback.configure(Map.of(QUOTA_POLICY_INTERVAL_PROP, 0));

        //Then
        verifyNoInteractions(executorService);
    }

    @Test
    void shouldScheduleQuotaPolicyTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);
        final Long interval = 10L;

        //When
        staticQuotaCallback.configure(Map.of(QUOTA_POLICY_INTERVAL_PROP, interval.intValue()));

        //Then
        verify(executorService).scheduleWithFixedDelay(isA(QuotaFactorPolicyTask.class), anyLong(), eq(interval), eq(TimeUnit.SECONDS));
    }

    @Test
    void shouldCancelExistingScheduleQuotaPolicyTask() {
        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);
        final ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        doReturn(scheduledFuture).when(executorService).scheduleWithFixedDelay(isA(QuotaFactorPolicyTask.class), anyLong(), anyLong(), any());
        final int interval = 10;
        staticQuotaCallback.configure(Map.of(QUOTA_POLICY_INTERVAL_PROP, interval));

        //When
        //Because calling configure again could potentially re-configure the scheduledTask it should be cancelled and re-scheduled
        staticQuotaCallback.configure(Map.of(QUOTA_POLICY_INTERVAL_PROP, interval));

        //Then
        verify(scheduledFuture).cancel(false);
    }

    @Test
    void shouldShutdownExecutorServiceOnClose() {

        //Given
        final ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(executorService, this::spyOnQuotaConfig, kafkaClientManager);
        when(executorService.shutdownNow()).thenReturn(List.of());

        //When
        staticQuotaCallback.close();

        //Then
        verify(executorService).shutdownNow();
    }

    @Test
    void shouldPropagateCloseToKafkaClientManager() {
        //Given
        final KafkaClientManager kafkaClientManager = mock(KafkaClientManager.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);

        //When
        staticQuotaCallback.close();

        //Then
        verify(kafkaClientManager).close();
    }

    @Test
    void shouldPropagateConfigurationToKafkaClientManager() {
        //Given
        final KafkaClientManager kafkaClientManager = mock(KafkaClientManager.class);
        final StaticQuotaCallback staticQuotaCallback = new StaticQuotaCallback(Executors.newSingleThreadScheduledExecutor(), this::spyOnQuotaConfig, kafkaClientManager);
        final Map<String, Object> configMap = Map.of();

        //When
        staticQuotaCallback.configure(configMap);

        //Then
        verify(kafkaClientManager).configure(eq(configMap));
    }

    private StaticQuotaConfig spyOnQuotaConfig(Map<String, ?> config, Boolean doLog) {
        return spyOnQuotaConfig(config, doLog, UnlimitedQuotaSupplier.UNLIMITED_QUOTA_SUPPLIER);
    }

    //TODO this is still a code smell.
    private StaticQuotaConfig spyOnQuotaConfig(Map<String, ?> config, Boolean doLog, QuotaFactorSupplier quotaFactorSupplier) {
        StaticQuotaConfig staticQuotaConfig = spy(new StaticQuotaConfig(config, doLog));
        staticQuotaConfig.withKafkaClientManager(kafkaClientManager);
        lenient().doReturn(quotaFactorSupplier).when(staticQuotaConfig).quotaFactorSupplier();
        lenient().doReturn((Supplier<Iterable<VolumeUsageMetrics>>) List::of).when(staticQuotaConfig).volumeUsageMetricsSupplier();
        lenient().doReturn((Consumer<VolumeUsageMetrics>) volumeUsageMetrics -> {
        }).when(staticQuotaConfig).volumeUsageMetricsPublisher();
        return staticQuotaConfig;
    }

    private static class FixedQuotaFactorSupplier implements QuotaFactorSupplier {

        private final double fixedFactor;

        private FixedQuotaFactorSupplier(double factor) {
            fixedFactor = factor;
        }

        @Override
        public void addUpdateListener(Runnable listener) {

        }

        @Override
        public Double get() {
            return fixedFactor;
        }
    }
}
