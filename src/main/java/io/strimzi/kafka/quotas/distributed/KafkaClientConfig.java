/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.distributed;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

class KafkaClientConfig extends AbstractConfig {

    public KafkaClientConfig(Map<String, ?> props, boolean doLog) {
        super(new ConfigDef()
                        .define(KafkaClientFactory.LISTENER_NAME_PROP, STRING, "replication-9091", LOW, "which listener to connect to")
                        .define(KafkaClientFactory.LISTENER_PORT_PROP, INT, 9091, LOW, "which port to connect to the listener on")
                        .define(KafkaClientFactory.LISTENER_PROTOCOL_PROP, STRING, "SSL", LOW, "what protocol to use when connecting to the listener listener"),
                props,
                doLog);
    }
}
