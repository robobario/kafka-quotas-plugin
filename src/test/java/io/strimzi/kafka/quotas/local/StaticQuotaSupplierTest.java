/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.local;

import java.util.Map;

import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StaticQuotaSupplierTest {

    public static final double PRODUCE_QUOTA = 1024.0;
    public static final double FETCH_QUOTA = 456.0;
    public static final double REQUEST_QUOTA = 123456.0;
    public static final double EPSILON = 0.00001;
    private StaticQuotaSupplier staticQuotaSupplier;

    @BeforeEach
    void setUp() {
        staticQuotaSupplier = new StaticQuotaSupplier(Map.of(
                ClientQuotaType.PRODUCE, Quota.upperBound(PRODUCE_QUOTA),
                ClientQuotaType.FETCH, Quota.upperBound(FETCH_QUOTA),
                ClientQuotaType.REQUEST, Quota.upperBound(REQUEST_QUOTA)));
    }

    @Test
    void shouldReturnConfiguredProduceQuota() {
        //Given

        //When
        final double actualQuota = staticQuotaSupplier.quotaFor(ClientQuotaType.PRODUCE, Map.of());

        //Then
        assertThat(actualQuota).isEqualTo(PRODUCE_QUOTA, Offset.offset(EPSILON));
    }

    @Test
    void shouldReturnConfiguredFetchQuota() {
        //Given

        //When
        final double actualQuota = staticQuotaSupplier.quotaFor(ClientQuotaType.FETCH, Map.of());

        //Then
        assertThat(actualQuota).isEqualTo(FETCH_QUOTA, Offset.offset(EPSILON));
    }
    @Test
    void shouldReturnConfiguredRequestQuota() {
        //Given

        //When
        final double actualQuota = staticQuotaSupplier.quotaFor(ClientQuotaType.REQUEST, Map.of());

        //Then
        assertThat(actualQuota).isEqualTo(REQUEST_QUOTA, Offset.offset(EPSILON));
    }
}
