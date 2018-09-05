package com.epam.bdcc.serde;

import com.epam.bdcc.htm.MonitoringRecord;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaJsonMonitoringRecordSerDeTest {

    @Test
    public void givenMonitoringRecord_whenUsingKafkaJSONSerDe_thenReadCorrectly() {

        MonitoringRecord originalMR = new MonitoringRecord();
        originalMR.setStateName("NY");
        originalMR.setPredictionNext(0.555);

        KafkaJsonMonitoringRecordSerDe serDe = new KafkaJsonMonitoringRecordSerDe();

        byte[] mrSerialized = serDe.serialize("topic", originalMR);
        MonitoringRecord deSerializedMR = serDe.deserialize("topic", mrSerialized);

        assertEquals(originalMR, deSerializedMR);
    }
}