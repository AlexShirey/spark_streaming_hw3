package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaKryoMonitoringRecordSerDeTest {

    @Test
    public void givenMonitoringRecord_whenUsingKafkaKryoSerDe_thenReadCorrectly() {

        MonitoringRecord originalMR = new MonitoringRecord();
        originalMR.setStateName("NY");
        originalMR.setPredictionNext(0.555);

        KafkaKryoMonitoringRecordSerDe serDe = new KafkaKryoMonitoringRecordSerDe();

        byte[] mrSerialized = serDe.serialize("topic", originalMR);
        MonitoringRecord deSerializedMR = serDe.deserialize("topic", mrSerialized);

        assertEquals(originalMR, deSerializedMR);
    }

    @Test
    public void givenMonitoringRecord_whenUsingInternalKryoSerializerOnly_thenReadCorrectly() {

        MonitoringRecord originalMR = new MonitoringRecord();
        originalMR.setStateName("NY");
        originalMR.setPredictionNext(0.555);

        Kryo kryo = new Kryo();
        kryo.register(MonitoringRecord.class, new KafkaKryoMonitoringRecordSerDe.KryoInternalSerializer());
        Output output = new Output(200);

        kryo.writeObject(output, originalMR);
        MonitoringRecord deSerializedMR = kryo.readObject(new Input(output.getBuffer()), MonitoringRecord.class);

        assertEquals(originalMR, deSerializedMR);
    }
}