/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import io.strimzi.kafka.quotas.types.Volume;
import org.assertj.core.data.Offset;

public class TestUtils {
    public static final double EPSILON = 0.00001;

    public static final Offset<Double> EPSILON_OFFSET =  Offset.offset(TestUtils.EPSILON);

    public static Volume newVolumeWith(long consumedCapacity) {
        return new Volume("Disk One", 20L, consumedCapacity);
    }
}
