package com.epam.bdcc.serde;

import com.epam.bdcc.htm.HTMNetwork;
import com.epam.bdcc.htm.MonitoringRecord;
import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

public class SparkKryoHTMRegistrator implements KryoRegistrator {

    public SparkKryoHTMRegistrator() {
    }

    @Override
    public void registerClasses(Kryo kryo) {
        // the rest HTM.java internal classes support Persistence API (with preSerialize/postDeserialize methods),
        // therefore we'll create the seralizers which will use HTMObjectInput/HTMObjectOutput (wrappers on top of fast-serialization)
        // which WILL call the preSerialize/postDeserialize
        SparkKryoHTMSerializer.registerSerializers(kryo);

        //TODO : We should register the top level classes with kryo
        kryo.register(HTMNetwork.class, new SparkKryoHTMSerializer());
        kryo.register(MonitoringRecord.class);

//        kryo.register(Object[].class);
//        kryo.register(scala.collection.mutable.WrappedArray.ofRef.class);
//        kryo.register(Long[].class);
//        kryo.register(org.apache.spark.streaming.rdd.MapWithStateRDDRecord.class);
//        kryo.register(org.apache.spark.streaming.util.OpenHashMapBasedStateMap.class);
//        try {
//            kryo.register(Class.forName("scala.reflect.ManifestFactory$$anon$2"));
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        kryo.register(java.lang.Class.class);
//        kryo.register(Object.class);
//        kryo.register(org.apache.spark.streaming.util.OpenHashMapBasedStateMap.LimitMarker.class);
    }
}
