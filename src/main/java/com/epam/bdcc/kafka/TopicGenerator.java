package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

/**
 * This class is a Kafka Producer -
 * it reads input data file, creates MonitoringRecord object from each line and
 * send this raw objects to kafka topic
 */
public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) throws IOException {

//         load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean.parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final int batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));
            final long batchSleep = Long.parseLong(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG));
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);

            //TODO : Read the file (one_device_2015-2017.csv) and push records to Kafka raw topic.

            //Read sample file and get Iterable from it
            Iterable<String[]> recordLines = getRecordsFromCSV(sampleFile, skipHeader);

            //to check performance between JSON and Kryo Serializers
            long before = System.currentTimeMillis();

            //Send each line to Kafka raw topic
            sendRecordsToKafka(recordLines, batchSize, batchSleep, topicName);

            long result = System.currentTimeMillis() - before;
            String serializer = PropertiesLoader.getKafkaProducerProperties().get(VALUE_SERIALIZER_CLASS_CONFIG).toString();

            //result is approximately the same for both Serializers, but JSON is even a little bit faster then Kryo!
            //JSON result (avg of 10 measures, without sleeping) - 717ms
            //Kryo result (avg of 10 measures, without sleeping) - 770ms
            LOGGER.info("Time to send all records from '" + sampleFile + "' to Kafka '" + topicName + "' topic (Serializer - " + serializer + ") is: " + result);
        }
    }

    /**
     * Reads csv file and return Iterable<String[]>
     *
     * @param path       - path to read from
     * @param skipHeader - read first line or not
     * @return - CSVReader - Iterable<String[]> object that contains all lines
     */
    private static CSVReader getRecordsFromCSV(String path, boolean skipHeader) throws IOException {

        CSVReader recordLines;
        if (skipHeader) {
            recordLines = new CSVReaderBuilder(new FileReader(path)).build();
        } else {
            recordLines = new CSVReaderBuilder(new FileReader(path)).withSkipLines(1).build();
        }
        return recordLines;
    }

    /**
     * Create MonitoringRecord object from each line,
     * and sent it to Kafka topic.
     * After each batchSize of records sent, sleeps for batchSleep duration
     *
     * @param recordLines - input lines
     * @param batchSize   - number of records after which we can make pause
     * @param batchSleep  - duration, how long we will sleep before send another record
     * @param topicName   - topic to send records to
     */
    private static void sendRecordsToKafka(Iterable<String[]> recordLines, int batchSize, long batchSleep, String topicName) {

        KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
        int recordsSent = 0;

        for (String[] line : recordLines) {
            MonitoringRecord record = new MonitoringRecord(line);
            System.out.println(recordsSent + " " + record);
            producer.send(new ProducerRecord<>(topicName, KafkaHelper.getKey(record), record));
            recordsSent++;

            if (recordsSent % batchSize == 0) {
                try {
                    System.out.println("sleeping");
                    Thread.sleep(batchSleep);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        producer.close();
    }
}
