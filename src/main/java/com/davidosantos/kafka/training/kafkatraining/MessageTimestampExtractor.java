package com.davidosantos.kafka.training.kafkatraining;

import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MessageTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        MessageTimingInterface timestamp = (MessageTimingInterface) record.value();
        if (timestamp != null && timestamp.getMessageTiming() != null) {
            String timestampStr = timestamp.getMessageTiming();
            return Instant.parse(timestampStr).toEpochMilli();
        }
        return partitionTime;
    }

}
