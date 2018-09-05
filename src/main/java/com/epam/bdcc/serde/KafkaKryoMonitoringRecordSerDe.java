package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Kafka SerDe - Serializer and Deserializer for MonitoringRecord objects
 * using Kryo for better performance
 */
public class KafkaKryoMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {
    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.addDefaultSerializer(MonitoringRecord.class, new KryoInternalSerializer());
            return kryo;
        }
    };

    //nothing to do
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    //Serializes MonitoringRecord object as a byte array using Kryo
    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {

        ByteBufferOutput output = new ByteBufferOutput(200);
        kryos.get().writeObject(output, data);
        return output.toBytes();
    }

    //DeSerializes MonitoringRecord object from a byte array using Kryo
    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {
        try {
            return kryos.get().readObject(new ByteBufferInput(data), MonitoringRecord.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    //nothing to do
    @Override
    public void close() {
    }

    /**
     * Kryo MonitoringRecord internal serializer
     */
    public static class KryoInternalSerializer extends com.esotericsoftware.kryo.Serializer<MonitoringRecord> {

        @Override
        public void write(Kryo kryo, Output output, MonitoringRecord mr) {

            output.writeString(mr.getStateCode());
            output.writeString(mr.getCountyCode());
            output.writeString(mr.getSiteNum());
            output.writeString(mr.getParameterCode());
            output.writeString(mr.getPoc());
            output.writeString(mr.getLatitude());
            output.writeString(mr.getLongitude());
            output.writeString(mr.getDatum());
            output.writeString(mr.getParameterName());
            output.writeString(mr.getDateLocal());
            output.writeString(mr.getTimeLocal());
            output.writeString(mr.getDateGMT());
            output.writeString(mr.getTimeGMT());
            output.writeString(mr.getSampleMeasurement());
            output.writeString(mr.getUnitsOfMeasure());
            output.writeString(mr.getMdl());
            output.writeString(mr.getUncertainty());
            output.writeString(mr.getQualifier());
            output.writeString(mr.getMethodType());
            output.writeString(mr.getMethodCode());
            output.writeString(mr.getMethodName());
            output.writeString(mr.getStateName());
            output.writeString(mr.getCountyName());
            output.writeString(mr.getDateOfLastChange());

            output.writeDouble(mr.getPrediction());
            output.writeDouble(mr.getError());
            output.writeDouble(mr.getAnomaly());
            output.writeDouble(mr.getPredictionNext());
        }

        @Override
        public MonitoringRecord read(Kryo kryo, Input input, Class<MonitoringRecord> type) {

            String[] line = new String[24];

            for (int i = 0; i < line.length; i++) {
                line[i] = input.readString();
            }

            double prediction = input.readDouble();
            double error = input.readDouble();
            double anomaly = input.readDouble();
            double predictionNext = input.readDouble();

            MonitoringRecord mr = new MonitoringRecord(line);
            mr.setPrediction(prediction);
            mr.setError(error);
            mr.setAnomaly(anomaly);
            mr.setPredictionNext(predictionNext);

            return mr;
        }
    }
}
