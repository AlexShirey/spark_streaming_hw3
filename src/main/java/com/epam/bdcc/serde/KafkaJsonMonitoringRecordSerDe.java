package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Kafka SerDe - Serializer and Deserializer for MonitoringRecord objects
 * using com.fasterxml.jackson library
 */
public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    //nothing to do
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //TODO : Add implementation for configure, if needed
    }

    //Serializes MonitoringRecord object as a byte array
    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {

        //TODO : Add implementation for serialization
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing data", e);
        }
    }

    //DeSerializes MonitoringRecord object from a byte array
    @Override
    public MonitoringRecord deserialize(String topic, byte[] bytes) {

        //TODO : Add implementation for deserialization
        if (bytes == null) {
            return null;
        }

        MonitoringRecord record;
        try {
            record = objectMapper.readValue(bytes, MonitoringRecord.class);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
        return record;
    }

    //nothing to do
    @Override
    public void close() {
        //TODO : Add implementation for close, if needed
    }
}