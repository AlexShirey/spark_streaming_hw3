package com.epam.bdcc.spark;

import com.epam.bdcc.htm.HTMNetwork;
import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.htm.ResultState;
import com.epam.bdcc.kafka.KafkaHelper;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Properties;


public class AnomalyDetector implements GlobalConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.epam");

    /**
     * TODO :
     * 1. Define Spark configuration (register serializers, if needed)
     * 2. Initialize streaming context with checkpoint directory
     * 3. Read records from kafka topic "monitoring20" (com.epam.bdcc.kafka.KafkaHelper can help)
     * 4. Organized records by key and map with HTMNetwork state for each device
     * (com.epam.bdcc.kafka.KafkaHelper.getKey - unique key for the device),
     * for detecting anomalies and updating HTMNetwork state (com.epam.bdcc.spark.AnomalyDetector.mappingFunc can help)
     * 5. Send enriched records to topic "monitoringEnriched2" for further visualization
     **/
    public static void main(String[] args) throws Exception {
        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String rawTopicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String enrichedTopicName = applicationProperties.getProperty(KAFKA_ENRICHED_TOPIC_CONFIG);
            final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
            final Duration batchDuration = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));
            final Duration checkpointInterval = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_CHECKPOINT_INTERVAL_CONFIG)));

            //TODO : Create your pipeline here!!!

//             Function to create JavaStreamingContext without any output operations
//             (used to detect the new context)
            Function0<JavaStreamingContext> createContextFunc =
                    () -> createContext(appName, rawTopicName, enrichedTopicName, checkpointDir, batchDuration, checkpointInterval);

            JavaStreamingContext ssc =
                    JavaStreamingContext.getOrCreate(checkpointDir, createContextFunc);

//            JavaStreamingContext ssc = createContext(appName, rawTopicName, enrichedTopicName, checkpointDir, batchDuration, checkpointInterval);

            ssc.sparkContext().setLogLevel("ERROR");

            ssc.start();
            ssc.awaitTermination();
        }
    }

    //Creates context and do all job
    private static JavaStreamingContext createContext(String appName, String rawTopicName, String enrichedTopicName, String checkpointDir, Duration batchDuration, Duration checkpointInterval) {

        // If you do not see this printed, that means the StreamingContext has been loaded
        // from the new checkpoint
        System.out.println("Creating new context");

        //Set up the configuration, enable Kryo serialization, register HTMNetwork class
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]")
//                .set("spark.streaming.backpressure.enabled", "true")
//                .set(SPARK_INTERNAL_SERIALIZER_CONFIG, "org.apache.spark.serializer.KryoSerializer")
                .set(SPARK_KRYO_REGISTRATOR_CONFIG, "com.epam.bdcc.serde.SparkKryoHTMRegistrator");

        //Create streaming context, set the checkpoint directory
        JavaStreamingContext ssc = new JavaStreamingContext(conf, batchDuration);
        ssc.checkpoint(checkpointDir);

        //Create dstream from Kafka rawTopic (each given Kafka partition corresponds to an RDD partition) to get raw MonitoringRecords
        JavaInputDStream<ConsumerRecord<String, MonitoringRecord>> dstream = createDstreamFromKafka(ssc, rawTopicName);

        //Convert ConsumerRecords to Pair DStream that contains record key and MonitoringRecord object to use in next step
        JavaPairDStream<String, MonitoringRecord> pairs = dstream.mapToPair(s -> new Tuple2<>(s.key(), s.value()));
//        pairs.print();
//        pairs.count().print();

        //Apply mappingFunc to every MonitoringRecord object to enrich them
        JavaMapWithStateDStream<String, MonitoringRecord, HTMNetwork, MonitoringRecord> enrichedDStream = pairs.mapWithState(StateSpec.function(mappingFunc));
//        enrichedDStream.print();
//        enrichedDStream.count().print();

        //Transform <String, MonitoringRecord, HTMNetwork, MonitoringRecord> to <MonitoringRecord> stream
        JavaDStream<MonitoringRecord> enrichedMR = enrichedDStream.transform(rdd -> rdd);
        enrichedMR.checkpoint(checkpointInterval);

        //Send enriched MonitoringRecords to Kafka enriched topic (as kafka-producer)
        sendEnrichedMRsToKafka(enrichedMR, enrichedTopicName);

        return ssc;
    }

    /**
     * Java constructor for a DStream where
     * each given Kafka topic/partition corresponds to an RDD partition.
     */
    private static JavaInputDStream<ConsumerRecord<String, MonitoringRecord>> createDstreamFromKafka(JavaStreamingContext streamingContext, String topic) {

        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(), KafkaHelper.createConsumerStrategy(topic)
        );
    }

    /**
     * Send each enriched record to Kafka enriched topic.
     * Producer is created on the worker on RDD partition to avoid creation overheads over many records.
     */
    private static void sendEnrichedMRsToKafka(JavaDStream<MonitoringRecord> enrichedDStream, String enrichedTopicName) {

        enrichedDStream.foreachRDD(rdd -> {
            if (rdd.isEmpty()) {
                return;
            }
            rdd.foreachPartition(partitionOfRecords -> {
                KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
                while (partitionOfRecords.hasNext()) {
                    MonitoringRecord enrichedMR = partitionOfRecords.next();
                    producer.send(new ProducerRecord<String, MonitoringRecord>(enrichedTopicName, KafkaHelper.getKey(enrichedMR), enrichedMR));
                }
                producer.close();
            });
        });
    }

    //Function to enrich records using HTMNetwork state
    private static Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> mappingFunc =
            (deviceID, recordOpt, state) -> {
                // case 0: timeout
                if (!recordOpt.isPresent())
                    return null;

                // either new or existing device
                if (!state.exists()) {
                    LOGGER.info("Creating new HTMNetwork");
                    state.update(new HTMNetwork(deviceID));
                }
                HTMNetwork htmNetwork = state.get();
                String stateDeviceID = htmNetwork.getId();
                if (!stateDeviceID.equals(deviceID))
                    throw new Exception("Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s");
                MonitoringRecord record = recordOpt.get();

                // get the value of DT and Measurement and pass it to the HTM
                HashMap<String, Object> m = new java.util.HashMap<>();
                m.put("DT", DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern("YY-MM-dd HH:mm")));
                m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
                ResultState rs = htmNetwork.compute(m);
                record.setPrediction(rs.getPrediction());
                record.setError(rs.getError());
                record.setAnomaly(rs.getAnomaly());
                record.setPredictionNext(rs.getPredictionNext());

                return record;
            };
}


