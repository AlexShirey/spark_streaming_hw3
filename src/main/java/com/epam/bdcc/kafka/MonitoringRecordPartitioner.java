package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Kafka producer partitioner.
 * The partitioning strategy:
 * - partition by month that is parsed from Object (MonitoringRecord) value
 * - in this task number of partitions for Producer is 10, so each partition will take approximately the same number of records
 * - default partitioner is called if a key is null or a value is not a MonitoringRecord (but for this task it should never happens)
 */
public class MonitoringRecordPartitioner extends DefaultPartitioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonitoringRecordPartitioner.class);

    //returns the number of partition to which record will be send
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //TODO : Add implementation for MonitoringRecord Partitioner
        if (value instanceof MonitoringRecord) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();
            if (keyBytes == null) {
                return super.partition(topic, key, null, value, valueBytes, cluster);
            }
            try {
                String month = ((MonitoringRecord) value).getDateGMT().substring(5, 7);
                return MonitoringRecordPartitioner.toPositive(month.hashCode()) % numPartitions;
            } catch (Exception e) {
                LOGGER.error("exception while getting DateGMT from a record:" + e.getMessage());
                throw new IllegalArgumentException(e);
            }
        } else {
            return super.partition(topic, key, keyBytes, value, valueBytes, cluster);
        }
    }

    //nothing to do
    public void close() {
        //TODO : Add implementation for close, if needed
    }

    //nothing to do
    public void configure(Map<String, ?> map) {
        //TODO : Add implementation for configure, if needed
    }


    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against 0x7fffffff which is not its absolutely
     * value.
     * Note: changing this method in the future will possibly cause partition selection not to be
     * compatible with the existing messages already placed on a partition.
     *
     * @param number a given number
     * @return a positive number.
     * @see DefaultPartitioner
     */
    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }
}