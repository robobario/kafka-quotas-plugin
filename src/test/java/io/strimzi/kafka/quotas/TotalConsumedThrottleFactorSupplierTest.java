package io.strimzi.kafka.quotas;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

@SuppressWarnings("deprecation")
class TotalConsumedThrottleFactorSupplierTest {

    @Test
    public void testListenersNotifiedOnChange() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 900L);
        Runnable runnable = Mockito.mock(Runnable.class);
        supplier.addUpdateListener(runnable);
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 100L)));
        Mockito.verify(runnable).run();
    }

    @Test
    public void testListenerNotNotifiedIfTotalConsumedUnchanged() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 900L);
        Runnable runnable = Mockito.mock(Runnable.class);
        supplier.addUpdateListener(runnable);
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 100L)));
        Mockito.verify(runnable).run();
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 100L)));
        Mockito.verifyNoMoreInteractions(runnable);
    }

    @Test
    public void testListenerNotifiedIfTotalConsumedChanged() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 900L);
        Runnable runnable = Mockito.mock(Runnable.class);
        supplier.addUpdateListener(runnable);
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 100L)));
        Mockito.verify(runnable).run();
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 50L)));
        Mockito.verify(runnable, Mockito.times(2)).run();
        Mockito.verifyNoMoreInteractions(runnable);
    }

    @Test
    public void testHardLimitViolation() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 900L);
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 100L)));
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.0, Offset.offset(0.00001d));
    }

    @Test
    public void testSoftLimitViolation() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 800L);
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 150L)));
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.5, Offset.offset(0.00001d));
    }

    @Test
    public void testSoftLimitViolationLowerBound() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 800L);
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 199L)));
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.99, Offset.offset(0.00001d));
    }

    @Test
    public void testSoftLimitViolationUpperBound() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 800L);
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 101L)));
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.01, Offset.offset(0.00001d));
    }

    @Test
    public void testHardLimitViolationAcrossMultipleVolumes() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 900L);
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 50L),
                new Volume("1", "/dir2", 1000L, 50L)));
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.0, Offset.offset(0.00001d));
    }

    @Test
    public void testHardLimitViolationRecovery() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 900L);
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 100L)));
        supplier.accept(List.of(new Volume("1", "/dir", 1000L, 1000L)));
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(1.0, Offset.offset(0.00001d));
    }

    @Test
    public void testThrottleFactorDefaultsToOpen() {
        TotalConsumedThrottleFactorSupplier supplier = new TotalConsumedThrottleFactorSupplier(900L, 900L);
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(1.0, Offset.offset(0.00001d));
    }

}